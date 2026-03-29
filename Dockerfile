FROM python:3.11-slim

WORKDIR /app

# 安装 cron（用于定时任务管理 API）
RUN apt-get update && apt-get install -y --no-install-recommends cron && rm -rf /var/lib/apt/lists/*

# 使用中国镜像源安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

# 复制项目代码
COPY . .

# 入口脚本：启动 cron + web
RUN chmod +x entrypoint.sh

# 暴露端口
EXPOSE 8080

# 启动（cron daemon + web）
CMD ["/app/entrypoint.sh"]
