from cryptography.fernet import Fernet
key = Fernet.generate_key().decode('utf-8')

print(key)