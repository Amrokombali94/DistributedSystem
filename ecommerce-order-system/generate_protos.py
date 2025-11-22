#!/usr/bin/env python3

import os
import subprocess
import sys


def generate_proto_files():
    proto_dir = "protos"
    services = ["user", "product", "order", "payment", "shipping", "two_phase_commit"]

    for service in services:
        output_dir = f"{service}_service"
        os.makedirs(output_dir, exist_ok=True)

        # 1️⃣ Generate the service-specific proto (user.proto, product.proto, ...)
        service_proto_file = os.path.join(proto_dir, f"{service}.proto")
        if os.path.exists(service_proto_file):
            cmd = [
                "python", "-m", "grpc_tools.protoc",
                f"--proto_path={proto_dir}",
                f"--python_out={output_dir}",
                f"--grpc_python_out={output_dir}",
                service_proto_file,
            ]

            print(f"Generating code for {service}.proto...")
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                print(f"❌ Error generating {service}.proto: {result.stderr}")
            else:
                print(f"✅ Successfully generated {service}_pb2.py and {service}_pb2_grpc.py")
        else:
            print(f"⚠️  Skipping {service}.proto (file not found at {service_proto_file})")

if __name__ == "__main__":
    generate_proto_files()
