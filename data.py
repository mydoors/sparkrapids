import csv
import random
import time

# 函数来生成随机数据
def generate_random_data(num_records, num_unique):
    data = []
    protocols = ["SSH", "TCP", "UDP"]
    for i in range(1, num_records + 1 - num_unique):
        frame_number = i
        frame_time_epoch = time.time() + random.random()
        ip_src = f"192.168.{random.randint(1, 5)}.{random.randint(1, 255)}"
        tcp_srcport = random.randint(1024, 65535)
        ip_dst = f"192.168.{random.randint(1, 5)}.{random.randint(1, 255)}"
        tcp_dstport = random.randint(1024, 65535)
        protocol = random.choice(protocols)

        data.append([frame_number, frame_time_epoch, ip_src, tcp_srcport, ip_dst, tcp_dstport, protocol])
    
    # 添加记录，确保具有相同源IP和目标IP但协议不同
    for i in range(num_records - num_unique, num_records):
        frame_number = i + 1
        frame_time_epoch = time.time() + random.random()
        ip = f"192.168.{random.randint(1, 5)}.{random.randint(1, 255)}"
        tcp_port = random.randint(1024, 65535)
        protocol1, protocol2 = random.sample(protocols, 2)  # 随机选择两个不同的协议

        data.append([frame_number, frame_time_epoch, ip, tcp_port, ip, tcp_port, protocol1])
        frame_number += 1
        data.append([frame_number, frame_time_epoch, ip, tcp_port, ip, tcp_port, protocol2])

    return data

# 主程序
def main():
    num_records = 10000000  # 总记录数
    num_unique = 1000  # 保证至少这么多记录有相同源和目标IP但不同协议
    filename = "network_data.csv"

    # 生成数据
    random_data = generate_random_data(num_records, num_unique)

    # 写入CSV文件
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['frame.number', 'frame.time_epoch', 'ip.src', 'tcp.srcport', 'ip.dst', 'tcp.dstport', '_ws.col.Protocol'])
        writer.writerows(random_data)

    print(f"数据已生成并保存到 {filename}")

if __name__ == "__main__":
    main()
