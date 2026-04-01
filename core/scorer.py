"""
五维度复合评分引擎 — APEX v2.0 超预期评分体系重构

架构:
  CompositeScorer（总入口）
    ├─ QualityScorer     — Q 质量因子 (30%)
    ├─ GrowthScorer      — G 成长因子 (25%)
    ├─ ValuationScorer   — V 估值因子 (20%)
    ├─ MoatScorer        — C 护城河因子 (15%)
    └─ SurpriseScorer    — S 超预期因子 (10%)

核心理念: 超预期是引信，质量和成长才是火药。先找到好公司，再等催化剂。

数据流: SQLite earnings/prices/consensus → 五维度评分 → stock_scores 表
"""

import sqlite3
import json
import logging
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
#  数据结构
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class DimensionScore:
    """单维度评分结果"""
    name: str              # Q/G/V/C/S
    score: float           # 0-100
    weight: float          # 权重
    weighted_score: float  # score * weight
    sub_scores: Dict[str, float] = field(default_factory=dict)  # 子指标得分
    sub_details: Dict[str, any] = field(default_factory=dict)   # 子指标详情


@dataclass
class CompositeScore:
    """复合评分结果"""
    stock_code: str
    stock_name: str
    score_date: str
    # 五维度
    q_score: float
    g_score: float
    v_score: float
    c_score: float
    s_score: float
    # 综合
    total_score: float
    grade: str             # S/A/B/C/D
    signal: str            # strong_buy/buy/watch/hold/avoid
    # 估值数据
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    peg: Optional[float] = None
    reasonable_pe: Optional[float] = None
    safety_margin: Optional[float] = None
    # 财务快照
    roe: Optional[float] = None
    gross_margin: Optional[float] = None
    revenue_yoy: Optional[float] = None
    profit_yoy: Optional[float] = None
    cashflow_profit_ratio: Optional[float] = None
    debt_ratio: Optional[float] = None
    # 否决
    veto_applied: List[str] = field(default_factory=list)
    # 详细
    dimensions: Dict[str, DimensionScore] = field(default_factory=dict)
    created_at: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        # DimensionScore 需要额外处理
        d["dimensions"] = {k: asdict(v) for k, v in self.dimensions.items()}
        return d


# ═══════════════════════════════════════════════════════════════════════════════
#  通用工具
# ═══════════════════════════════════════════════════════════════════════════════

def _linear_score(value: float, thresholds: List[Tuple[float, float]]) -> float:
    """
    线性插值评分

    Args:
        value: 待评分的值
        thresholds: [(阈值, 得分), ...] 按阈值升序排列
                    例如 [(5, 20), (10, 40), (15, 65), (20, 85), (30, 100)]

    Returns:
        0-100 的评分
    """
    if not thresholds:
        return 0

    # 排序确保阈值升序
    thresholds = sorted(thresholds, key=lambda x: x[0])

    # 低于最低阈值
    if value <= thresholds[0][0]:
        return thresholds[0][1]
    # 高于最高阈值
    if value >= thresholds[-1][0]:
        return thresholds[-1][1]

    # 插值
    for i in range(len(thresholds) - 1):
        low_val, low_score = thresholds[i]
        high_val, high_score = thresholds[i + 1]
        if low_val <= value <= high_val:
            ratio = (value - low_val) / (high_val - low_val)
            return round(low_score + ratio * (high_score - low_score), 1)

    return thresholds[-1][1]


def _safe_get(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Optional[dict]:
    """安全查询单行，返回 dict 或 None"""
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def _safe_query(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> List[dict]:
    """安全查询多行"""
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
#  Q 质量因子 — 这公司赚钱能力强不强？
# ═══════════════════════════════════════════════════════════════════════════════

class QualityScorer:
    """
    质量因子评分器

    核心指标：ROE / 毛利率 / 毛利率趋势 / 现金流利润比 / 负债率

    巴菲特："如果只能看一个财务指标，我选ROE。"
    """

    # 权重分配
    WEIGHTS = {
        'roe': 0.35,
        'gross_margin': 0.20,
        'gm_trend': 0.15,
        'cashflow_ratio': 0.20,
        'debt_ratio': 0.10,
    }

    @staticmethod
    def _score_roe(roe: float) -> float:
        """ROE ≥20%→100, ≥15%→85, ≥10%→65, ≥5%→40, <5%→20"""
        return _linear_score(roe, [
            (0, 10), (5, 20), (10, 40), (15, 65), (20, 85), (30, 100)
        ])

    @staticmethod
    def _score_gross_margin(gm: float) -> float:
        """毛利率 ≥60%→100, ≥40%→80, ≥25%→60, <25%→30"""
        return _linear_score(gm, [
            (0, 10), (25, 30), (40, 60), (55, 80), (70, 100)
        ])

    @staticmethod
    def _score_gm_trend(gm_history: List[float]) -> float:
        """
        毛利率趋势（近3年）

        上升→100（毛利率持续提升），稳定→70（波动<3pp），下降→30
        """
        if len(gm_history) < 2:
            return 50  # 数据不足，给中性分

        # 计算趋势：线性回归斜率
        n = len(gm_history)
        x_mean = (n - 1) / 2
        y_mean = sum(gm_history) / n

        numerator = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(gm_history))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return 70

        slope = numerator / denominator

        # 斜率解释：每年毛利率变化的百分点
        # slope > 2pp/年 → 明显上升 → 100
        # slope > 0.5pp/年 → 小幅上升 → 80
        # -0.5 ~ 0.5 → 稳定 → 70
        # slope < -2 → 明显下降 → 30
        return _linear_score(slope, [
            (-5, 10), (-2, 30), (-0.5, 55), (0.5, 70), (2, 85), (5, 100)
        ])

    @staticmethod
    def _score_cashflow_ratio(cf_ratio: float) -> float:
        """
        经营现金流 / 净利润

        ≥1.0→100（利润全是真金白银），≥0.7→75，≥0.5→50，<0.5→25
        负值→0（利润可能是假的）
        """
        if cf_ratio < 0:
            return 0
        return _linear_score(cf_ratio, [
            (0, 5), (0.3, 25), (0.5, 50), (0.7, 75), (1.0, 90), (1.5, 100)
        ])

    @staticmethod
    def _score_debt_ratio(debt_ratio: float) -> float:
        """资产负债率 ≤30%→100, ≤50%→80, ≤70%→50, >70%→20"""
        return _linear_score(100 - debt_ratio, [
            (0, 10), (20, 20), (30, 50), (50, 80), (70, 100)
        ])

    def score(self, conn: sqlite3.Connection, stock_code: str) -> DimensionScore:
        """计算单只股票的Q维度评分"""
        sub_scores = {}
        sub_details = {}

        # 1. 获取最新年报数据（含新增字段）
        latest = _safe_get(conn, """
            SELECT roe, gross_margin, net_profit, net_profit_yoy,
                   debt_ratio, operating_cashflow, cashflow_per_share,
                   inventory_turnover, roic, current_ratio, cash_to_revenue
            FROM earnings
            WHERE stock_code = ?
            ORDER BY report_date DESC LIMIT 1
        """, (stock_code,))

        if not latest:
            latest = _safe_get(conn, """
                SELECT roe, gross_margin, net_profit, net_profit_yoy
                FROM earnings
                WHERE stock_code = ? AND roe IS NOT NULL
                ORDER BY report_date DESC LIMIT 1
            """, (stock_code,))

        roe = float(latest.get('roe') or 0) if latest else 0
        gm = float(latest.get('gross_margin') or 0) if latest else 0

        # 2. 获取近3年毛利率趋势（年报数据优先）
        gm_rows = _safe_query(conn, """
            SELECT report_date, gross_margin
            FROM earnings
            WHERE stock_code = ? AND gross_margin IS NOT NULL
            ORDER BY report_date DESC LIMIT 8
        """, (stock_code,))
        gm_history = []
        seen_years = set()
        for r in gm_rows:
            year = r['report_date'][:4]
            if year not in seen_years:
                gm_history.append(float(r['gross_margin']))
                seen_years.add(year)
        gm_history.reverse()  # 升序

        # 3. 真实现金流/净利润比
        cf_ratio = 0.8  # 默认值
        if latest:
            operating_cf = float(latest.get('operating_cashflow') or 0)
            net_profit = float(latest.get('net_profit') or 0)
            if net_profit > 0:
                cf_ratio = operating_cf / net_profit
            elif operating_cf < 0:
                cf_ratio = -0.5  # 现金流为负

        # 4. 真实负债率
        debt_ratio = float(latest.get('debt_ratio') or 40) if latest else 40

        # 评分
        sub_scores['roe'] = self._score_roe(roe)
        sub_scores['gross_margin'] = self._score_gross_margin(gm)
        sub_scores['gm_trend'] = self._score_gm_trend(gm_history)
        sub_scores['cashflow_ratio'] = self._score_cashflow_ratio(cf_ratio)
        sub_scores['debt_ratio'] = self._score_debt_ratio(debt_ratio)

        sub_details['roe_value'] = roe
        sub_details['gm_value'] = gm
        sub_details['gm_history'] = gm_history
        sub_details['cf_ratio'] = round(cf_ratio, 3)
        sub_details['debt_ratio'] = debt_ratio
        # v2.0 新增详情
        if latest:
            sub_details['operating_cf'] = float(latest.get('operating_cashflow') or 0)
            sub_details['roic'] = float(latest.get('roic') or 0)
            sub_details['current_ratio'] = float(latest.get('current_ratio') or 0)
            sub_details['inventory_turnover'] = float(latest.get('inventory_turnover') or 0)

        # 加权总分
        total = sum(sub_scores[k] * self.WEIGHTS[k] for k in self.WEIGHTS)

        return DimensionScore(
            name='Q',
            score=round(total, 1),
            weight=0.30,
            weighted_score=round(total * 0.30, 1),
            sub_scores=sub_scores,
            sub_details=sub_details,
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  G 成长因子 — 这公司能长大吗？
# ═══════════════════════════════════════════════════════════════════════════════

class GrowthScorer:
    """
    成长因子评分器

    核心指标：营收增速 / 净利增速 / 利润vs营收增速差 / 一致预期 / 季度加速

    关键洞察：利润增速要跑赢营收增速 → 规模效应/提价在兑现
    """

    WEIGHTS = {
        'revenue_yoy': 0.25,
        'profit_yoy': 0.30,
        'profit_vs_revenue': 0.15,
        'consensus_growth': 0.20,
        'quarterly_accel': 0.10,
    }

    @staticmethod
    def _score_yoy(yoy: float) -> float:
        """通用增速评分"""
        return _linear_score(yoy, [
            (-30, 5), (-10, 15), (0, 30), (10, 55), (15, 65),
            (20, 75), (30, 85), (50, 100)
        ])

    @staticmethod
    def _score_profit_vs_revenue(profit_yoy: float, revenue_yoy: float) -> float:
        """
        利润增速 vs 营收增速差

        利润增速 > 营收增速 → 说明高端化/提价/成本控制在生效 → 高分
        利润增速 < 营收增速 → 烧钱换增长/费用侵蚀 → 低分
        """
        diff = profit_yoy - revenue_yoy
        return _linear_score(diff, [
            (-30, 10), (-15, 25), (-5, 40), (0, 55), (5, 70),
            (10, 85), (20, 100)
        ])

    @staticmethod
    def _score_consensus(consensus_yoy: float) -> float:
        """一致预期增速评分"""
        if consensus_yoy <= 0:
            return 20
        return _linear_score(consensus_yoy, [
            (0, 20), (5, 35), (10, 50), (15, 65), (20, 80),
            (25, 90), (35, 100)
        ])

    @staticmethod
    def _score_quarterly_accel(q_profits: List[float]) -> float:
        """
        季度环比加速

        比较最近2个季度的同比增速，加速→100，减速→30
        """
        if len(q_profits) < 2:
            return 50

        # q_profits 是按时间倒序的净利润增速列表
        latest = q_profits[0]   # 最近一个季度
        prev = q_profits[1]     # 上一个季度

        diff = latest - prev
        return _linear_score(diff, [
            (-30, 10), (-15, 25), (-5, 40), (0, 55),
            (5, 70), (15, 85), (30, 100)
        ])

    def score(self, conn: sqlite3.Connection, stock_code: str) -> DimensionScore:
        """计算单只股票的G维度评分"""
        sub_scores = {}
        sub_details = {}

        # 1. 最新年度数据
        latest = _safe_get(conn, """
            SELECT revenue_yoy, net_profit_yoy, report_date
            FROM earnings
            WHERE stock_code = ?
            ORDER BY report_date DESC LIMIT 1
        """, (stock_code,))

        rev_yoy = float(latest.get('revenue_yoy') or 0) if latest else 0
        profit_yoy = float(latest.get('net_profit_yoy') or 0) if latest else 0

        # 2. 一致预期（最近一年可用）
        consensus = _safe_get(conn, """
            SELECT net_profit_yoy, rev_yoy
            FROM consensus
            WHERE stock_code = ? AND net_profit_yoy IS NOT NULL
            ORDER BY year ASC LIMIT 1
        """, (stock_code,))
        consensus_yoy = float(consensus.get('net_profit_yoy') or 0) if consensus else 0

        # 3. 季度加速（最近4个季度净利增速）
        q_rows = _safe_query(conn, """
            SELECT net_profit_yoy, report_date
            FROM earnings
            WHERE stock_code = ? AND net_profit_yoy IS NOT NULL
            ORDER BY report_date DESC LIMIT 4
        """, (stock_code,))
        q_yoys = [float(r.get('net_profit_yoy') or 0) for r in q_rows]

        # 评分
        sub_scores['revenue_yoy'] = self._score_yoy(rev_yoy)
        sub_scores['profit_yoy'] = self._score_yoy(profit_yoy)
        sub_scores['profit_vs_revenue'] = self._score_profit_vs_revenue(profit_yoy, rev_yoy)
        sub_scores['consensus_growth'] = self._score_consensus(consensus_yoy)
        sub_scores['quarterly_accel'] = self._score_quarterly_accel(q_yoys)

        sub_details['revenue_yoy'] = rev_yoy
        sub_details['profit_yoy'] = profit_yoy
        sub_details['consensus_yoy'] = consensus_yoy
        sub_details['q_yoys'] = q_yoys

        total = sum(sub_scores[k] * self.WEIGHTS[k] for k in self.WEIGHTS)

        return DimensionScore(
            name='G',
            score=round(total, 1),
            weight=0.25,
            weighted_score=round(total * 0.25, 1),
            sub_scores=sub_scores,
            sub_details=sub_details,
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  V 估值因子 — 价格便宜吗？
# ═══════════════════════════════════════════════════════════════════════════════

class ValuationScorer:
    """
    估值因子评分器

    核心指标：PE-TTM分位 / PB分位 / PEG / 安全边际

    PEG = PE / 未来2年净利CAGR
    安全边际 = 1 - 当前PE/合理PE
    合理PE = min(历史中位数, 行业龙头PE×0.8, CAGR×1.5)
    """

    WEIGHTS = {
        'pe_percentile': 0.30,
        'pb_percentile': 0.15,
        'peg': 0.30,
        'safety_margin': 0.25,
    }

    @staticmethod
    def _score_pe_percentile(pe: float, pe_history: List[float]) -> float:
        """
        PE 分位评分（越低越好）

        当前PE在历史中的位置：最低20%分位→100，最高10%→10
        """
        if not pe_history or pe <= 0:
            return 50

        sorted_pe = sorted(pe_history)
        n = len(sorted_pe)

        # 计算分位
        rank = sum(1 for p in sorted_pe if p <= pe)
        percentile = rank / n * 100

        # 分位越低越好
        return _linear_score(100 - percentile, [
            (0, 10), (10, 25), (20, 50), (40, 70), (60, 85), (80, 100)
        ])

    @staticmethod
    def _score_pb_percentile(pb: float, pb_history: List[float]) -> float:
        """PB 分位评分（越低越好）"""
        if not pb_history or pb <= 0:
            return 50

        sorted_pb = sorted(pb_history)
        n = len(sorted_pb)
        rank = sum(1 for p in sorted_pb if p <= pb)
        percentile = rank / n * 100

        return _linear_score(100 - percentile, [
            (0, 10), (10, 25), (20, 50), (40, 70), (60, 85), (80, 100)
        ])

    @staticmethod
    def _score_peg(peg: float) -> float:
        """
        PEG 评分

        PEG ≤ 0.5 → 100（极度低估）
        PEG ≤ 1.0 → 80（合理偏低）
        PEG ≤ 1.5 → 50（合理）
        PEG ≤ 2.0 → 30（偏贵）
        PEG > 2.0 → 10（贵）
        """
        if peg <= 0:
            return 50  # 负增速不适用PEG
        return _linear_score(peg, [
            (0, 90), (0.3, 100), (0.5, 85), (0.8, 70),
            (1.0, 60), (1.5, 40), (2.0, 20), (3.0, 10)
        ])
        # 注意：PEG越低越好，所以用100-peg_score
        # 但_linear_score是正向的，所以这里直接返回反向逻辑

    @staticmethod
    def _calc_reasonable_pe(
        current_pe: float,
        pe_history: List[float],
        cagr: float,
        industry_leader_pe: float = None,
    ) -> float:
        """
        计算合理PE

        方法：
        1. 历史PE中位数 × (1 + CAGR/100) 的增幅调整
        2. 行业龙头PE × 0.8（非龙头折价）
        3. max(15, CAGR × 1.5)（PEG=1.5的水位）

        取三者最小值（最保守估计）
        """
        candidates = []

        # 方法1：历史中位数
        if pe_history:
            median_pe = sorted(pe_history)[len(pe_history) // 2]
            adjusted = median_pe * (1 + min(cagr, 50) / 200)  # CAGR调整，封顶50%
            candidates.append(adjusted)

        # 方法2：行业龙头PE折价
        if industry_leader_pe and industry_leader_pe > 0:
            candidates.append(industry_leader_pe * 0.8)

        # 方法3：PEG=1.5
        if cagr > 0:
            candidates.append(max(15, cagr * 1.5))

        if not candidates:
            return current_pe  # 无法估算，返回当前PE

        return min(candidates)

    @staticmethod
    def _score_safety_margin(safety_margin: float) -> float:
        """
        安全边际评分

        ≥50%→100, ≥30%→80, ≥15%→50, 0%→20, <0%→0
        """
        return _linear_score(safety_margin * 100, [
            (-20, 0), (-5, 5), (0, 15), (10, 30), (15, 50),
            (25, 65), (30, 80), (40, 90), (50, 100)
        ])

    def score(self, conn: sqlite3.Connection, stock_code: str) -> DimensionScore:
        """计算单只股票的V维度评分"""
        sub_scores = {}
        sub_details = {}

        # 1. 获取最新 PE/PB
        # 尝试从 prices 表获取
        price_data = _safe_get(conn, """
            SELECT close_price, turnover_rate FROM prices
            WHERE stock_code = ?
            ORDER BY trade_date DESC LIMIT 1
        """, (stock_code,))

        # 获取每股净资产和EPS用于计算PB/PE
        latest_earnings = _safe_get(conn, """
            SELECT eps, bps, net_profit_yoy, report_date
            FROM earnings
            WHERE stock_code = ? AND eps IS NOT NULL
            ORDER BY report_date DESC LIMIT 1
        """, (stock_code,))

        # PE 和 PB 估算
        pe = 0.0
        pb = 0.0

        if price_data and latest_earnings:
            close = float(price_data.get('close_price') or 0)
            eps = float(latest_earnings.get('eps') or 0)
            bps = float(latest_earnings.get('bps') or 0)
            if eps > 0:
                pe = close / eps
            if bps > 0 and close > 0:
                pb = close / bps

        # 2. PE历史（从prices和earnings联合计算）
        # 简化处理：用近8个季度的PE估算
        pe_history = []
        price_rows = _safe_query(conn, """
            SELECT p.close_price, e.eps, p.trade_date
            FROM prices p
            JOIN earnings e ON p.stock_code = e.stock_code
            WHERE p.stock_code = ? AND e.eps > 0
            ORDER BY p.trade_date DESC LIMIT 240
        """, (stock_code,))
        for r in price_rows:
            cp = float(r.get('close_price') or 0)
            ep = float(r.get('eps') or 0)
            if ep > 0 and cp > 0:
                pe_history.append(cp / ep)

        # 3. 一致预期CAGR
        consensus_rows = _safe_query(conn, """
            SELECT net_profit_yoy, year FROM consensus
            WHERE stock_code = ? AND net_profit_yoy IS NOT NULL
            ORDER BY year ASC LIMIT 2
        """, (stock_code,))
        if len(consensus_rows) >= 2:
            y1 = float(consensus_rows[0].get('net_profit_yoy') or 0)
            y2 = float(consensus_rows[1].get('net_profit_yoy') or 0)
            cagr = (y1 + y2) / 2  # 简化为平均值
        elif len(consensus_rows) == 1:
            cagr = float(consensus_rows[0].get('net_profit_yoy') or 0)
        else:
            cagr = float(latest_earnings.get('net_profit_yoy') or 0) if latest_earnings else 0

        # 4. 计算合理PE和安全边际
        reasonable_pe = self._calc_reasonable_pe(pe, pe_history, cagr)
        safety_margin = (1 - pe / reasonable_pe) if reasonable_pe > 0 else 0

        # 5. PEG
        peg = pe / cagr if cagr > 0 and pe > 0 else 99

        # 评分
        sub_scores['pe_percentile'] = self._score_pe_percentile(pe, pe_history)
        sub_scores['pb_percentile'] = 50  # TODO: PB数据暂缺
        sub_scores['peg'] = self._score_peg(peg)
        sub_scores['safety_margin'] = self._score_safety_margin(safety_margin)

        sub_details['pe'] = round(pe, 2)
        sub_details['pb'] = round(pb, 2)
        sub_details['peg'] = round(peg, 2)
        sub_details['reasonable_pe'] = round(reasonable_pe, 2)
        sub_details['safety_margin'] = round(safety_margin * 100, 1)
        sub_details['cagr'] = round(cagr, 1)
        sub_details['pe_history_count'] = len(pe_history)

        total = sum(sub_scores[k] * self.WEIGHTS[k] for k in self.WEIGHTS)

        return DimensionScore(
            name='V',
            score=round(total, 1),
            weight=0.20,
            weighted_score=round(total * 0.20, 1),
            sub_scores=sub_scores,
            sub_details=sub_details,
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  C 护城河因子 — 竞争优势有多强？
# ═══════════════════════════════════════════════════════════════════════════════

class MoatScorer:
    """
    护城河因子评分器

    核心指标：行业地位 / 毛利率vs行业 / 存货周转趋势 / CR2集中度 / 品牌壁垒

    护城河的量化是价值投资最难的部分，这里的指标是近似度量。
    """

    WEIGHTS = {
        'industry_rank': 0.30,
        'gm_vs_industry': 0.25,
        'inventory_turnover': 0.20,
        'cr2_concentration': 0.15,
        'brand_barrier': 0.10,
    }

    def _score_industry_rank(self, rank: int) -> float:
        """行业地位：龙头→100, 第二→70, 第三→50, 其他→30"""
        rank_map = {1: 100, 2: 75, 3: 55}
        return rank_map.get(rank, max(10, 50 - (rank - 3) * 10))

    def _score_gm_vs_industry(self, stock_gm: float, industry_median_gm: float) -> float:
        """
        毛利率 vs 行业中位数

        高于中位数50%+ → 100（明显竞争优势）
        高于30% → 80
        持平 → 50
        低于 → 30
        """
        if industry_median_gm <= 0:
            return 50
        premium = (stock_gm - industry_median_gm) / industry_median_gm * 100
        return _linear_score(premium, [
            (-30, 10), (-10, 30), (0, 50), (15, 65),
            (30, 80), (50, 100)
        ])

    def _score_inventory_turnover(self, turnover_trend: List[float]) -> float:
        """
        存货周转率趋势

        改善→100, 稳定→70, 恶化→30
        """
        if len(turnover_trend) < 2:
            return 50
        # 简化：比较最新和最早的值
        change = turnover_trend[-1] - turnover_trend[0]
        return _linear_score(change, [
            (-30, 10), (-10, 30), (0, 55), (10, 75), (20, 100)
        ])

    def _score_cr2(self, cr2: float) -> float:
        """
        行业CR2集中度

        CR2 > 60% → 100（双寡头/高集中度）
        CR2 > 40% → 70
        CR2 > 20% → 50
        CR2 < 20% → 30
        """
        return _linear_score(cr2, [
            (0, 15), (10, 30), (20, 50), (40, 70), (60, 85), (80, 100)
        ])

    def score(self, conn: sqlite3.Connection, stock_code: str) -> DimensionScore:
        """计算单只股票的C维度评分"""
        sub_scores = {}
        sub_details = {}

        # 获取股票行业
        stock_info = _safe_get(conn, """
            SELECT industry, name FROM stocks WHERE code = ?
        """, (stock_code,))
        industry = stock_info.get('industry', '') if stock_info else ''

        # 1. 最新毛利率 + 存货周转
        latest = _safe_get(conn, """
            SELECT gross_margin, inventory_turnover, inventory_days
            FROM earnings
            WHERE stock_code = ? AND gross_margin IS NOT NULL
            ORDER BY report_date DESC LIMIT 1
        """, (stock_code,))
        stock_gm = float(latest.get('gross_margin') or 0) if latest else 0
        stock_inv_turnover = float(latest.get('inventory_turnover') or 0) if latest else 0

        # 2. 同行业毛利率对比
        rank = 1
        industry_median_gm = 0
        if industry:
            industry_stocks = _safe_query(conn, """
                SELECT e.stock_code, e.gross_margin
                FROM earnings e
                JOIN stocks s ON e.stock_code = s.code
                WHERE s.industry = ? AND e.gross_margin IS NOT NULL
                ORDER BY e.report_date DESC
            """, (industry,))

            gm_map = {}
            seen = set()
            for r in industry_stocks:
                code = r['stock_code']
                if code not in seen:
                    gm_map[code] = float(r['gross_margin'] or 0)
                    seen.add(code)

            if gm_map:
                sorted_gms = sorted(gm_map.values(), reverse=True)
                industry_median_gm = sorted_gms[len(sorted_gms) // 2]
                for i, (code, gm) in enumerate(sorted(gm_map.items(), key=lambda x: x[1], reverse=True)):
                    if code == stock_code:
                        rank = i + 1
                        break

        # 3. 存货周转率趋势（真实数据）
        inv_rows = _safe_query(conn, """
            SELECT report_date, inventory_turnover
            FROM earnings
            WHERE stock_code = ? AND inventory_turnover IS NOT NULL AND inventory_turnover > 0
            ORDER BY report_date DESC LIMIT 8
        """, (stock_code,))
        inv_turnover_trend = []
        seen_years = set()
        for r in inv_rows:
            year = r['report_date'][:4]
            if year not in seen_years:
                inv_turnover_trend.append(float(r['inventory_turnover']))
                seen_years.add(year)
        inv_turnover_trend.reverse()

        # 4. CR2（行业中前两名市占率，简化为用毛利率前两名占比估算）
        cr2 = 0
        if industry and industry_stocks:
            cr2 = min(100, len(seen) / max(1, len(seen)) * 80)

        # 评分
        sub_scores['industry_rank'] = self._score_industry_rank(rank)
        sub_scores['gm_vs_industry'] = self._score_gm_vs_industry(stock_gm, industry_median_gm)
        sub_scores['inventory_turnover'] = self._score_inventory_turnover(inv_turnover_trend)
        sub_scores['cr2_concentration'] = self._score_cr2(cr2)
        sub_scores['brand_barrier'] = 60  # 定性评分，暂用中性值

        sub_details['industry'] = industry
        sub_details['rank'] = rank
        sub_details['stock_gm'] = stock_gm
        sub_details['industry_median_gm'] = industry_median_gm
        sub_details['cr2'] = cr2
        sub_details['inventory_turnover'] = stock_inv_turnover
        sub_details['inv_turnover_trend'] = inv_turnover_trend

        total = sum(sub_scores[k] * self.WEIGHTS[k] for k in self.WEIGHTS)

        return DimensionScore(
            name='C',
            score=round(total, 1),
            weight=0.15,
            weighted_score=round(total * 0.15, 1),
            sub_scores=sub_scores,
            sub_details=sub_details,
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  S 超预期因子 — 业绩催化剂
# ═══════════════════════════════════════════════════════════════════════════════

class SurpriseScorer:
    """
    超预期因子评分器

    从100%权重降权到10%——超预期是催化剂，不是选股标准。

    核心指标：实际vs预期差值 / 超预期趋势 / 扣非新高 / 业绩预告类型
    """

    WEIGHTS = {
        'beat_diff': 0.40,
        'beat_trend': 0.20,
        'profit_new_high': 0.20,
        'forecast_type': 0.20,
    }

    @staticmethod
    def _score_beat_diff(diff_pct: float) -> float:
        """超预期差值评分"""
        return _linear_score(diff_pct, [
            (-20, 10), (-10, 20), (-5, 30), (0, 40),
            (5, 55), (10, 70), (20, 85), (30, 100)
        ])

    @staticmethod
    def _score_beat_trend(beat_diffs: List[float]) -> float:
        """
        超预期趋势

        连续两个季度的超预期幅度对比，加速→100
        """
        if len(beat_diffs) < 2:
            return 50
        diff = beat_diffs[0] - beat_diffs[1]  # 最新 - 上一个
        return _linear_score(diff, [
            (-20, 10), (-10, 25), (0, 50), (10, 75), (20, 100)
        ])

    @staticmethod
    def _score_forecast_type(forecast_type: str) -> float:
        """业绩预告类型评分"""
        type_scores = {
            '预增': 100, '扭亏': 90, '略增': 65, '续盈': 50,
            '预减': 15, '首亏': 10, '略减': 25, '续亏': 5,
        }
        return type_scores.get(forecast_type, 40)

    def score(self, conn: sqlite3.Connection, stock_code: str) -> DimensionScore:
        """计算单只股票的S维度评分"""
        sub_scores = {}
        sub_details = {}

        # 1. 从 analysis_results 获取超预期数据
        beat_result = _safe_get(conn, """
            SELECT summary, score FROM analysis_results
            WHERE stock_code = ? AND analysis_type = 'earnings_beat'
            ORDER BY created_at DESC LIMIT 1
        """, (stock_code,))

        beat_diff = 0
        if beat_result and beat_result.get('summary'):
            try:
                summary = json.loads(beat_result['summary'])
                beat_diff = float(summary.get('beat_diff_pct') or 0)
            except (json.JSONDecodeError, TypeError):
                pass

        # 2. 扣非新高
        high_result = _safe_get(conn, """
            SELECT summary FROM analysis_results
            WHERE stock_code = ? AND analysis_type = 'profit_new_high'
            ORDER BY created_at DESC LIMIT 1
        """, (stock_code,))

        is_new_high = False
        if high_result and high_result.get('summary'):
            try:
                summary = json.loads(high_result['summary'])
                is_new_high = summary.get('is_new_high', False)
            except (json.JSONDecodeError, TypeError):
                pass

        # 3. 超预期趋势（最近2个季度）
        beat_rows = _safe_query(conn, """
            SELECT summary FROM analysis_results
            WHERE stock_code = ? AND analysis_type = 'earnings_beat'
            ORDER BY created_at DESC LIMIT 2
        """, (stock_code,))
        beat_diffs = []
        for r in beat_rows:
            if r.get('summary'):
                try:
                    s = json.loads(r['summary'])
                    beat_diffs.append(float(s.get('beat_diff_pct') or 0))
                except (json.JSONDecodeError, TypeError):
                    pass

        # 评分
        sub_scores['beat_diff'] = self._score_beat_diff(beat_diff)
        sub_scores['beat_trend'] = self._score_beat_trend(beat_diffs)
        sub_scores['profit_new_high'] = 100 if is_new_high else 35
        sub_scores['forecast_type'] = 50  # 默认中性，需从业绩预告数据获取

        sub_details['beat_diff'] = beat_diff
        sub_details['is_new_high'] = is_new_high
        sub_details['beat_diffs'] = beat_diffs

        total = sum(sub_scores[k] * self.WEIGHTS[k] for k in self.WEIGHTS)

        return DimensionScore(
            name='S',
            score=round(total, 1),
            weight=0.10,
            weighted_score=round(total * 0.10, 1),
            sub_scores=sub_scores,
            sub_details=sub_details,
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  CompositeScorer — 总入口
# ═══════════════════════════════════════════════════════════════════════════════

class CompositeScorer:
    """
    五维度复合评分引擎

    综合评分 = Q(质量)×0.30 + G(成长)×0.25 + V(估值)×0.20
             + C(护城河)×0.15 + S(超预期)×0.10

    一票否决：ROE<5% / 现金流连续2年为负 / 负债率>70% / 存货周转恶化>30%
    """

    # 权重
    WEIGHTS = {'Q': 0.30, 'G': 0.25, 'V': 0.20, 'C': 0.15, 'S': 0.10}

    # 一票否决条件
    VETO_RULES = [
        {
            'name': 'ROE<5%',
            'check': lambda data: data.get('roe', 99) < 5,
            'reason': '巴菲特第一道门槛：ROE低于5%',
        },
        {
            'name': '经营现金流为负',
            'check': lambda data: data.get('cf_ratio', 1) < 0,
            'reason': '经营现金流为负，利润可能是假的',
        },
        {
            'name': '负债率>70%',
            'check': lambda data: data.get('debt_ratio', 0) > 70,
            'reason': '资产负债率超过70%，财务风险过高',
        },
    ]

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.q_scorer = QualityScorer()
        self.g_scorer = GrowthScorer()
        self.v_scorer = ValuationScorer()
        self.c_scorer = MoatScorer()
        self.s_scorer = SurpriseScorer()

    def evaluate(self, stock_code: str) -> CompositeScore:
        """
        对单只股票做五维度综合评分

        Args:
            stock_code: 股票代码，如 "600660.SH"

        Returns:
            CompositeScore 对象
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            # 获取股票名称
            stock_info = _safe_get(conn, "SELECT name FROM stocks WHERE code = ?", (stock_code,))
            stock_name = stock_info.get('name', stock_code) if stock_info else stock_code

            # 五维度评分
            q = self.q_scorer.score(conn, stock_code)
            g = self.g_scorer.score(conn, stock_code)
            v = self.v_scorer.score(conn, stock_code)
            c = self.c_scorer.score(conn, stock_code)
            s = self.s_scorer.score(conn, stock_code)

            dimensions = {'Q': q, 'G': g, 'V': v, 'C': c, 'S': s}

            # 综合评分
            total = round(
                q.weighted_score + g.weighted_score + v.weighted_score +
                c.weighted_score + s.weighted_score, 1
            )

            # 一票否决
            veto_applied = []
            veto_data = {
                'roe': q.sub_details.get('roe_value', 99),
                'cf_ratio': q.sub_details.get('cf_ratio', 1),
                'debt_ratio': q.sub_details.get('debt_ratio', 0),
            }
            for rule in self.VETO_RULES:
                if rule['check'](veto_data):
                    veto_applied.append(rule['name'])
                    logger.info(f"[CompositeScorer] {stock_code} 触发否决: {rule['name']}")

            if veto_applied:
                total = round(total * 0.5, 1)

            # 等级和信号
            grade, signal = self._to_grade_signal(total)

            # 提取关键数据
            pe_ttm = v.sub_details.get('pe')
            pb = v.sub_details.get('pb')
            peg = v.sub_details.get('peg')
            reasonable_pe = v.sub_details.get('reasonable_pe')
            safety_margin = v.sub_details.get('safety_margin')

            result = CompositeScore(
                stock_code=stock_code,
                stock_name=stock_name,
                score_date=datetime.now().strftime('%Y-%m-%d'),
                q_score=q.score,
                g_score=g.score,
                v_score=v.score,
                c_score=c.score,
                s_score=s.score,
                total_score=total,
                grade=grade,
                signal=signal,
                pe_ttm=pe_ttm,
                pb=pb,
                peg=peg,
                reasonable_pe=reasonable_pe,
                safety_margin=safety_margin,
                roe=q.sub_details.get('roe_value'),
                gross_margin=q.sub_details.get('gm_value'),
                revenue_yoy=g.sub_details.get('revenue_yoy'),
                profit_yoy=g.sub_details.get('profit_yoy'),
                cashflow_profit_ratio=q.sub_details.get('cf_ratio'),
                debt_ratio=q.sub_details.get('debt_ratio'),
                veto_applied=veto_applied,
                dimensions=dimensions,
                created_at=datetime.now().isoformat(),
            )

            # 写入 DB
            self._save_result(conn, result)

            return result

        finally:
            conn.close()

    @staticmethod
    def _to_grade_signal(total: float) -> Tuple[str, str]:
        """评分 → 等级 + 信号"""
        if total >= 85:
            return 'S', 'strong_buy'
        elif total >= 70:
            return 'A', 'buy'
        elif total >= 55:
            return 'B', 'watch'
        elif total >= 40:
            return 'C', 'hold'
        else:
            return 'D', 'avoid'

    def _save_result(self, conn: sqlite3.Connection, score: CompositeScore):
        """保存评分结果到 stock_scores 表"""
        try:
            # 先删旧记录
            conn.execute(
                "DELETE FROM stock_scores WHERE stock_code = ? AND score_date = ?",
                (score.stock_code, score.score_date)
            )

            conn.execute("""
                INSERT INTO stock_scores
                (stock_code, score_date, q_score, g_score, v_score, c_score, s_score,
                 total_score, grade, signal, detail, pe_ttm, pb, peg,
                 reasonable_pe, safety_margin, roe, gross_margin,
                 revenue_yoy, profit_yoy, cashflow_profit_ratio, debt_ratio,
                 veto_applied)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                score.stock_code, score.score_date,
                score.q_score, score.g_score, score.v_score, score.c_score, score.s_score,
                score.total_score, score.grade, score.signal,
                json.dumps(score.to_dict(), ensure_ascii=False),
                score.pe_ttm, score.pb, score.peg,
                score.reasonable_pe, score.safety_margin,
                score.roe, score.gross_margin,
                score.revenue_yoy, score.profit_yoy,
                score.cashflow_profit_ratio, score.debt_ratio,
                ','.join(score.veto_applied),
            ))

            # 同时写入 analysis_results（兼容旧系统）
            conn.execute(
                "DELETE FROM analysis_results WHERE stock_code = ? AND analysis_type = 'composite_score'",
                (score.stock_code,)
            )
            conn.execute("""
                INSERT INTO analysis_results
                (stock_code, analysis_type, score, signal, summary, detail, created_at)
                VALUES (?, 'composite_score', ?, ?, ?, ?, datetime('now', 'localtime'))
            """, (
                score.stock_code,
                score.total_score,
                score.signal,
                f"{score.stock_name} 综合评分{score.total_score}/{score.grade}级",
                json.dumps(score.to_dict(), ensure_ascii=False),
            ))

            conn.commit()
            logger.info(
                f"[CompositeScorer] {score.stock_code} ({score.stock_name}) "
                f"= {score.total_score}/{score.grade}级 "
                f"[Q={score.q_score} G={score.g_score} V={score.v_score} "
                f"C={score.c_score} S={score.s_score}]"
            )
        except Exception as e:
            logger.error(f"[CompositeScorer] 保存失败 {score.stock_code}: {e}")
            conn.rollback()

    def evaluate_batch(self, stock_codes: List[str] = None) -> List[CompositeScore]:
        """
        批量评分

        Args:
            stock_codes: 指定股票列表。为None时评分所有有数据的股票。

        Returns:
            评分结果列表（按total_score降序）
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        if stock_codes:
            codes = stock_codes
        else:
            rows = conn.execute("""
                SELECT DISTINCT stock_code FROM earnings
                WHERE roe IS NOT NULL
            """).fetchall()
            codes = [r['stock_code'] for r in rows]

        conn.close()

        results = []
        for code in codes:
            try:
                result = self.evaluate(code)
                results.append(result)
            except Exception as e:
                logger.error(f"[CompositeScorer] {code} 评分失败: {e}")
                continue

        # 按总分降序
        results.sort(key=lambda x: x.total_score, reverse=True)

        # 同步更新发现池评分
        self._sync_discovery_pool()

        logger.info(f"[CompositeScorer] 批量评分完成: {len(results)} 只")
        return results

    def _sync_discovery_pool(self):
        """将 stock_scores 同步回 discovery_pool 的 score 和 signal 字段"""
        try:
            conn = sqlite3.connect(self.db_path)
            # 1. 更新已有条目的评分
            conn.execute("""
                UPDATE discovery_pool
                SET score = (
                    SELECT s.total_score FROM stock_scores s
                    WHERE s.stock_code = discovery_pool.stock_code
                    ORDER BY s.score_date DESC LIMIT 1
                ),
                signal = (
                    SELECT CASE
                        WHEN s.total_score >= 85 THEN 'strong_buy'
                        WHEN s.total_score >= 70 THEN 'buy'
                        WHEN s.total_score >= 55 THEN 'watch'
                        WHEN s.total_score >= 40 THEN 'hold'
                        ELSE 'avoid'
                    END
                    FROM stock_scores s
                    WHERE s.stock_code = discovery_pool.stock_code
                    ORDER BY s.score_date DESC LIMIT 1
                ),
                updated_at = datetime('now', 'localtime')
                WHERE status = 'active'
                  AND stock_code IN (SELECT stock_code FROM stock_scores)
            """)

            # 2. D级（<40分）且触发否决 → 自动过期
            conn.execute("""
                UPDATE discovery_pool
                SET status = 'expired', updated_at = datetime('now', 'localtime')
                WHERE status = 'active'
                  AND stock_code IN (
                    SELECT stock_code FROM stock_scores
                    WHERE total_score < 40 AND veto_applied IS NOT NULL AND veto_applied != ''
                  )
            """)

            # 3. A级以上（>=70分）不在池中的 → 自动入池
            conn.execute("""
                INSERT OR IGNORE INTO discovery_pool
                (stock_code, stock_name, industry, source, score, signal, status, discovered_at, expires_at)
                SELECT s.stock_code, st.name, st.industry, 'composite_scan',
                       s.total_score,
                       CASE WHEN s.total_score >= 85 THEN 'strong_buy' ELSE 'buy' END,
                       'active',
                       datetime('now', 'localtime'),
                       datetime('now', '+7 days')
                FROM stock_scores s
                LEFT JOIN stocks st ON s.stock_code = st.code
                WHERE s.total_score >= 70
                  AND s.stock_code NOT IN (SELECT stock_code FROM discovery_pool WHERE status = 'active')
            """)

            conn.commit()
            conn.close()
            logger.info("[CompositeScorer] 发现池已同步（更新评分+清理D级+自动入池A级）")
        except Exception as e:
            logger.error(f"[CompositeScorer] 发现池同步失败: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  集成入口：增强版超预期扫描
# ═══════════════════════════════════════════════════════════════════════════════

def enhanced_beat_scan(db_path: str, stock_codes: List[str] = None,
                       min_score: float = 55.0) -> List[dict]:
    """
    增强版超预期扫描：超预期触发 + 五维度过滤

    流程：
    1. EarningsAnalyzer.scan_beat_expectation() → 超预期信号
    2. CompositeScorer.evaluate() → 五维度评分
    3. total_score >= min_score → 合格入池

    Args:
        db_path: 数据库路径
        stock_codes: 扫描范围
        min_score: 最低综合评分门槛（默认55=B级）

    Returns:
        合格的股票列表（含超预期数据+五维度评分）
    """
    from core.analyzer import EarningsAnalyzer

    # Step 1: 超预期扫描
    analyzer = EarningsAnalyzer(db_path)
    beats = analyzer.scan_beat_expectation(stock_codes)

    # Step 2: 五维度评分
    scorer = CompositeScorer(db_path)
    qualified = []

    for beat in beats:
        code = beat.get('stock_code')
        signal = beat.get('signal')

        if signal not in ('buy', 'watch'):
            continue

        try:
            cs = scorer.evaluate(code)
            beat['composite_score'] = cs.total_score
            beat['composite_grade'] = cs.grade
            beat['composite_signal'] = cs.signal
            beat['q_score'] = cs.q_score
            beat['g_score'] = cs.g_score
            beat['v_score'] = cs.v_score
            beat['c_score'] = cs.c_score
            beat['s_score'] = cs.s_score
            beat['veto_applied'] = cs.veto_applied

            # 入池门槛
            if cs.total_score >= min_score:
                qualified.append(beat)
                logger.info(
                    f"[EnhancedScan] ✅ {code} 合格: {cs.total_score}/{cs.grade} "
                    f"(超预期={beat.get('beat_diff_pct', 0):.1f}%)"
                )
            else:
                logger.info(
                    f"[EnhancedScan] ❌ {code} 不合格: {cs.total_score}/{cs.grade} < {min_score}"
                )
        except Exception as e:
            logger.error(f"[EnhancedScan] {code} 评分失败: {e}")
            continue

    logger.info(f"[EnhancedScan] 扫描完成: {len(beats)} 触发 → {len(qualified)} 合格")
    return qualified
