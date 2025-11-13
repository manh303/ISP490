import subprocess

print("Starting MinIO...")
subprocess.run("docker-compose up -d minio", shell=True)

print("\nMinIO started!")
print("Console: http://localhost:9001")
print("Username: minioadmin")
print("Password: minioadmin123")
print("\nAPI Endpoint: http://localhost:9000")
