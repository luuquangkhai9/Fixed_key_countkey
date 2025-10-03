FROM python:3.10
ENV TZ=Asia/Ho_Chi_Minh
# Đặt thư mục làm việc trong container
WORKDIR /usr/app/src
COPY . ./
RUN pip install --upgrade pip
RUN apt-get update

RUN pip install -r requirements.txt

# # Chạy ứng dụng FastAPI khi container được khởi động
# CMD ["bash", "-c", "python run.py & uvicorn main1:app --host 0.0.0.0 --port 5601 --reload"]

# Sao chép script start.sh vào container và đặt quyền thực thi
COPY startup.sh /usr/app/src/startup.sh
RUN chmod +x /usr/app/src/startup.sh

# Chạy script start.sh khi container được khởi động
CMD ["/bin/bash", "/usr/app/src/startup.sh"]
