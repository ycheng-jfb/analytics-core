# AWS CLI Setup in Git Bash / PyCharm (Windows)

This guide explains how to install and configure the AWS CLI so it works in both **Git Bash** and **PyCharm’s integrated terminal** on Windows.

---

## 1. Install AWS CLI v2
Choose one method:

- **Winget (PowerShell / CMD):**
  ```powershell
  winget install Amazon.AWSCLI
  ```

- **Chocolatey (admin PowerShell):**
  ```powershell
  choco install awscli -y
  ```

- **MSI installer (GUI):**  
  Download from [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and run.  
  Default install path:
  ```
  C:\Program Files\Amazon\AWSCLIV2\
  ```

---

## 2. Verify Installation
Open **Git Bash** and run:
```bash
"/c/Program Files/Amazon/AWSCLIV2/aws.exe" --version
```

Expected output:
```
aws-cli/2.x.x Python/3.x.x Windows/11 exe/AMD64
```

---

## 3. Fix PATH in Git Bash / PyCharm

### Option A — Add AWS CLI to PATH
Append this to `~/.bashrc`:
```bash
export PATH="/c/Program Files/Amazon/AWSCLIV2:$PATH"
```

Reload:
```bash
source ~/.bashrc
```

Check:
```bash
aws --version
```

---

## 4. Configure PyCharm Terminal
1. Go to **File → Settings → Tools → Terminal**.  
2. Set **Shell path** to:
   ```
   "C:\Program Files\Git\bin\bash.exe" -li
   ```
   The `-l -i` ensures Bash loads your `~/.bashrc`.

---

## 5. Authenticate with AWS

### Using Access Keys
```bash
aws configure
```

Enter:
- AWS Access Key ID  
- AWS Secret Access Key  
- Default region (e.g. `us-west-2`)  
- Output format (`json`, `table`, or `text`)

---

## 6. Test S3 Access
```bash
aws s3 ls                  # list all buckets
aws s3 ls s3://jfb.airflow.db   # list contents of a bucket
aws s3 ls s3://jfb.airflow.db --recursive --human-readable --summarize
```

---

## 7. Troubleshooting

- If `aws` works in standalone Git Bash but not in PyCharm → ensure `-li` is set in terminal settings.  
- If you see `bash: /c/Program: No such file or directory` → copy this to your .bashrc file `alias aws="\"/c/Program Files/Amazon/AWSCLIV2/aws.exe\""`.