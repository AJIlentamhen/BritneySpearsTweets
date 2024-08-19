FROM python:3.9-slim
WORKDIR /DockerProject/BritneyTweetApp
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 27017
ENV MONGO_URI mongodb://mongo:27017/
CMD ["python", "britney_tweet_script.py"]