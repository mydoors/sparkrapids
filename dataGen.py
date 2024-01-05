import json
import random
import datetime

def generate_ip_address():
    return f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}"

def generate_dns_record():
    record_types = ["A", "AAAA", "CNAME", "MX", "NS", "TXT", "SOA", "SRV", "PTR"]
    return {
        "type": random.choice(record_types),
        "name": f"example{random.randint(1, 100)}.com",
        "ttl": random.randint(100, 3600),
        # Other fields based on record type can be added here
    }

def generate_dns_query_response():
    return {
        "query": {
            "transaction_id": f"{random.randint(0, 65535):04x}",
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
            "queries": [generate_dns_record()],
        },
        "response": {
            "id": f"{random.randint(0, 65535):04x}",
            "flags": "0x8180",
            "opcode": "Standard query response",
            "response": True,
            "authoritative": True,
            "truncated": False,
            "recursion_desired": True,
            "recursion_available": True,
            "z_reserved": "0",
            "answer_authenticated": True,
            "non_authenticated_data": "Unacceptable",
            "reply_code": "No error",
            "answers": [generate_dns_record()],
        }
    }

def generate_full_dns_record():
    return {
        "network": {
            "frame": {
                "number": str(random.randint(1, 10000)),
                "on_wire_bytes": "124",
                "captured_bytes": "124",
                "interface": {
                    "id": "0",
                    "name": "en1",
                    "description": "Wi-Fi"
                },
                "encapsulation_type": "Ethernet (1)",
                "arrival_time": {
                    "local": datetime.datetime.now().isoformat(),
                    "utc": datetime.datetime.utcnow().isoformat(),
                    "epoch": str(datetime.datetime.now().timestamp())
                },
                "time_shift": "0.000000000",
                "time_since_first_frame": "1.077259000",
                "time_delta_previous_frame": "0.013875000",
                "protocols": ["eth", "ip", "udp", "dns"],
                "coloring_rule": {
                    "name": "UDP",
                    "string": "udp"
                }
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
                "source_ip": generate_ip_address(),
                "destination_ip": generate_ip_address()
            },
            "udp": {
                "source_port": "53",
                "destination_port": str(random.randint(1024, 65535)),
                "length": "90",
                "checksum": "0x7e70"
            }
        },
        "dns_records": [generate_dns_query_response() for _ in range(random.randint(1, 5))]
    }

def generate_dns_records(num_records, filename):
    with open(filename, 'w') as f:
        for _ in range(num_records):
            record = generate_full_dns_record()
            json.dump(record, f)
            f.write('\n')

generate_dns_records(10, 'dns_records.json')
