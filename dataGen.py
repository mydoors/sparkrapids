import json
import random
from datetime import datetime
from multiprocessing import Pool

def generate_dns_record():
    record_types = ["A", "AAAA", "CNAME", "MX", "NS", "TXT", "SOA", "SRV", "PTR"]
    record_type = random.choice(record_types)

    def generate_soa_record():
        return {
            "mname": f"ns{random.randint(1, 3)}.example.com",
            "rname": f"hostmaster.example.com",
            "serial": random.randint(1000000, 9999999),
            "refresh": random.randint(10000, 20000),
            "retry": random.randint(2000, 5000),
            "expire": random.randint(100000, 200000),
            "minimum": random.randint(3600, 7200)
        }

    def generate_srv_record():
        return {
            "target": f"service{random.randint(1, 3)}.example.com",
            "port": random.randint(80, 8080),
            "priority": random.randint(1, 10),
            "weight": random.randint(1, 10)
        }

    record = {
        "query": {
            "transaction_id": hex(random.randint(0x0000, 0xFFFF)),
            "flags": "0x0100",
            "opcode": "Standard query",
            "response": False,
            "authoritative": False,
            "truncated": False,
            "recursion_desired": True,
            "recursion_available": False,
            "z_reserved": "0",
            "answer_authenticated": False,
            "non_authenticated_data": "Unacceptable",
            "reply_code": "No error",
            "questions_count": 1,
            "queries": [{"type": record_type, "name": f"example{random.randint(1, 100)}.com", "ttl": random.randint(100, 5000)}]
        },
        "response": {
            "id": hex(random.randint(0x0000, 0xFFFF)),
            "name": f"example{random.randint(101, 200)}.com",
            "ttl": random.randint(100, 5000),
            "QR": "Response",
            "type": record_type,
            "rCode": "NoError",
            "section": "Answer",
            "dnsA": generate_ip() if record_type == "A" else None,
            "dnsAAAA": generate_ip(version=6) if record_type == "AAAA" else None,
            "dnsCNAME": f"cname{random.randint(1, 100)}.example.com" if record_type == "CNAME" else None,
            "dnsMX": f"mail{random.randint(1, 100)}.example.com" if record_type == "MX" else None,
            "dnsNS": f"ns{random.randint(1, 3)}.example.com" if record_type == "NS" else None,
            "dnsTXT": "v=spf1 include:_spf.example.com ~all" if record_type == "TXT" else None,
            "dnsSOA": generate_soa_record() if record_type == "SOA" else None,
            "dnsSRV": generate_srv_record() if record_type == "SRV" else None,
            "dnsPTR": f"host{random.randint(1, 100)}.example.com" if record_type == "PTR" else None
        }
    }

    return record

def generate_ip(version=4):
    if version == 4:
        return f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}"
    elif version == 6:
        return ":".join(f"{random.randint(0, 65535):x}" for _ in range(8))

def generate_network_info():
    return {
        "frame": {
            "number": str(random.randint(1, 10000)),
            "on_wire_bytes": "124",
            "captured_bytes": "124",
            "interface": {"id": "0", "name": "en1", "description": "Wi-Fi"},
            "encapsulation_type": "Ethernet (1)",
            "arrival_time": {"local": datetime.now().isoformat(), "utc": datetime.utcnow().isoformat(), "epoch": str(datetime.now().timestamp())},
            "protocols": ["eth", "ip", "udp", "dns"]
        },
        "ethernet": {
            "source_mac": "48:5f:08:2b:18:22",
            "destination_mac": "5c:1b:f4:a0:08:a4",
            "ethertype": "IPv4"
        },
        "ip": {
            "version": "4",
            "header_length": "20",
            "tos": "0x00",
            "total_length": "110",
            "identification": "0x0000",
            "flags": "0x0",
            "fragment_offset": "0",
            "ttl": "151",
            "protocol": "UDP",
            "header_checksum": "0x7d88",
            "source_ip": generate_ip(),
            "destination_ip": generate_ip()
        },
        "udp": {
            "source_port": "53",
            "destination_port": str(random.randint(1024, 65535)),
            "length": "90",
            "checksum": "0x7e70"
        }
    }

def generate_json_record(_):
    num_dns_records = random.randint(1, 5)
    dns_records = [generate_dns_record() for _ in range(num_dns_records)]
    network_info = generate_network_info()

    return {
        "network": network_info,
        "dns_records": dns_records
    }

# 新增的写入文件函数
def write_record(record, filename="dns_records.json"):
    with open(filename, "a") as file:
        json.dump(record, file)
        file.write("\n")

def main():
    num_records = 10  # 总共生成10个独立的JSON记录
    pool = Pool()  # 创建一个进程池

    records = pool.map(generate_json_record, [None] * num_records)
    pool.close()
    pool.join()

    for record in records:
        write_record(record)

if __name__ == "__main__":
    main()