# setup env
link setup :
    hadoop: https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
    spark: https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
    elastic: https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.15.2-linux-x86_64.tar.gz
    kafka: https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz

download :
- wget + link;
- tar -xzf *.tgz
luu y:

- khi cai hadoop: trong file etc/hadoop/hadoop-env.sh thi phai cau hinh java version << 16, khong thi se gap loi bao mat cua java



# crawl process
Quy trình tổng thể
- Khởi chạy crawler:
- Lấy danh sách URL cần crawl từ cấu hình hoặc một cơ sở dữ liệu.
Thu thập dữ liệu:
- Dùng Selenium để lấy dữ liệu từ trang.
- Xử lý các yếu tố động và thu thập thông tin.
Làm sạch và chuẩn hóa:
- Kiểm tra và làm sạch dữ liệu.
- Chuyển đổi dữ liệu về định dạng lưu trữ mong muốn.
Lưu vào HDFS:
- Ghi dữ liệu batch vào HDFS.
- Đảm bảo dữ liệu phân vùng phù hợp.
Báo cáo trạng thái:
- Gửi email hoặc ghi log về trạng thái crawl (số lượng thành công, thất bại).

## tool
1. Kiến trúc hệ thống
- Crawlers (Selenium, Scrapy, etc.): Thu thập dữ liệu từ các trang web.
- Kafka Cluster: Lưu trữ dữ liệu thô thu thập được từ crawlers, làm hàng đợi tin nhắn (message queue).
- Spark Streaming: Lấy dữ liệu từ Kafka để xử lý, làm sạch và lưu vào cơ sở dữ liệu hoặc hệ thống lưu trữ như HDFS/S3.
- HDFS/S3/Database: Lưu trữ dữ liệu sạch, sẵn sàng phân tích hoặc trực quan hóa.

## run
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties

## run kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --create --topic real_estate_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1