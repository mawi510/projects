import paramiko
import os

def run_remote_command(hostname, username, key, command):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(hostname, username=username, key_filename=key)
        stdin, stdout, stderr = ssh.exec_command(command)
        print(stdout.read().decode())
        print(stderr.read().decode())
    finally:
        ssh.close()

if __name__ == "__main__":
    hostname = os.getenv("EC2_PUBLIC_IP")
    username = os.getenv("EC2_USERNAME")
    key = os.getenv("EC2_SSH_KEY")
    command = "sudo certbot renew --nginx --cert-name promatchpredict.com"
    
    run_remote_command(hostname, username, key, command)
