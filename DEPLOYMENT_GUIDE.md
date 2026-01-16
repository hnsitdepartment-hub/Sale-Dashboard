# 🚀 Sales Dashboard - Streamlit Cloud Deployment Guide

## 📋 Overview

This guide provides step-by-step instructions for deploying the Sales Dashboard application to Streamlit Cloud. The deployment includes both the main dashboard (`app_dashboard.py`) and admin interface (`app_admin.py`).

## 🔧 Prerequisites

1. **GitHub Account** - Required for Streamlit Cloud deployment
2. **Database Access** - Your SQL Server databases must be accessible from Streamlit Cloud
3. **Python 3.9+** - For local testing
4. **Streamlit CLI** - Install with `pip install streamlit`

## 📁 Project Structure

```
sales_dashboard/
├── app_dashboard.py          # Main sales dashboard
├── app_admin.py              # Admin interface
├── db/
│   ├── connection.py          # Original connection module
│   └── connection_cloud.py    # Cloud-compatible connection module
├── .streamlit/
│   ├── config.toml            # Streamlit configuration
│   └── secrets.toml           # Database credentials (EXCLUDE FROM GIT)
├── requirements.txt          # Python dependencies
└── DEPLOYMENT_GUIDE.md       # This file
```

## 🛠️ Deployment Options

### Option 1: Streamlit Cloud (Recommended)

#### Step 1: Prepare Your Repository

```bash
# Make sure your repository is clean and up-to-date
git status
git add .
git commit -m "Prepare for Streamlit Cloud deployment"
git push origin main
```

#### Step 2: Set Up Streamlit Cloud

1. Go to [https://share.streamlit.io/](https://share.streamlit.io/)
2. Sign in with your GitHub account
3. Click **"New app"**
4. Select your repository: `hnsitdepartment-hub/Sale-Dashboard`
5. Configure your app:
   - **Main file path**: `app_dashboard.py`
   - **Branch**: `main`
   - **Python version**: `3.9` (recommended)

#### Step 3: Configure Database Access

**⚠️ CRITICAL STEP**: Your database must be accessible from Streamlit Cloud.

**Option A: Whitelist Streamlit Cloud IPs**
1. Find Streamlit Cloud IP ranges (check their documentation)
2. Add these IPs to your SQL Server firewall rules
3. Enable SQL Server authentication for remote access

**Option B: Use API Proxy (Recommended for Security)**
1. Set up a local API server (see `api_proxy.py` example below)
2. Expose it using ngrok: `ngrok http 5000`
3. Set the `DB_API_PROXY` environment variable in Streamlit Cloud

#### Step 4: Set Up Secrets

1. In Streamlit Cloud, go to your app settings
2. Click **"Secrets"** and paste the contents of `.streamlit/secrets.toml`
3. **IMPORTANT**: Remove sensitive credentials before committing to Git!

#### Step 5: Environment Variables

Set these environment variables in Streamlit Cloud settings:
- `STREAMLIT_CLOUD=true`
- `DB_API_PROXY=https://your-api-proxy.ngrok.io` (if using API proxy)

### Option 2: Local Deployment with ngrok

```bash
# Install requirements
pip install -r requirements.txt

# Run the dashboard
streamlit run app_dashboard.py

# Create public tunnel
ngrok http 8501
```

### Option 3: Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501
CMD ["streamlit", "run", "app_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

Build and run:
```bash
docker build -t sales-dashboard .
docker run -p 8501:8501 sales-dashboard
```

## 🔐 Database Configuration

### Direct Connection (If Firewall Allows)

The application will automatically try to connect directly to:
- Candelahns DB: `103.86.55.183,2001`
- KDS DB: `localhost`
- Fresh Pick DB: `103.86.55.183,2002`

### API Proxy (Recommended for Cloud)

Create `api_proxy.py`:

```python
from flask import Flask, request, jsonify
import pyodbc
import pandas as pd
import os

app = Flask(__name__)

# Load credentials from environment variables
DB_SERVER = os.environ.get('DB_SERVER', '103.86.55.183,2001')
DB_NAME = os.environ.get('DB_NAME', 'Candelahns')
DB_USER = os.environ.get('DB_USER', 'ReadOnlyUser')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '902729@Rafy')

def get_db_connection():
    conn_str = f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}"
    return pyodbc.connect(conn_str)

@app.route('/api/query', methods=['POST'])
def query_database():
    data = request.json
    query = data['query']
    params = data.get('params', [])

    try:
        conn = get_db_connection()
        df = pd.read_sql(query, conn, params=params)
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

Run the API proxy:
```bash
python api_proxy.py
ngrok http 5000
```

## 🚨 Troubleshooting

### Common Issues & Solutions

**1. Database Connection Failed**
- ✅ Check firewall rules
- ✅ Verify SQL Server allows remote connections
- ✅ Test credentials locally first
- ✅ Use API proxy if direct connection fails

**2. pyodbc.Error on Streamlit Cloud**
- ✅ Install required ODBC drivers
- ✅ Use API proxy as fallback
- ✅ Check connection strings

**3. App Not Updating After Changes**
- ✅ Streamlit Cloud auto-deploys from GitHub
- ✅ Make sure changes are pushed to main branch
- ✅ Check deployment logs for errors

## 📊 Monitoring & Maintenance

### Streamlit Cloud Dashboard
- Monitor app performance
- Check logs for errors
- View usage statistics

### Database Monitoring
- Monitor connection counts
- Check query performance
- Set up alerts for failed connections

## 🔄 Migration Guide

### From Local to Cloud

1. **Test locally first** with `streamlit run app_dashboard.py`
2. **Set up API proxy** for secure database access
3. **Configure secrets** in Streamlit Cloud
4. **Deploy and monitor** the application
5. **Gradually migrate** users to the cloud version

## 🎯 Best Practices

1. **Never commit secrets** to GitHub
2. **Use API proxy** for production deployments
3. **Monitor database connections** from cloud
4. **Implement rate limiting** on API proxy
5. **Regularly update** dependencies

## 📞 Support

For deployment issues:
1. Check Streamlit Cloud documentation
2. Review database connection logs
3. Test API proxy locally first
4. Contact your database administrator for firewall rules

---

**🎉 Your Sales Dashboard is now ready for cloud deployment!**

Choose the deployment option that best fits your infrastructure and security requirements. The cloud-compatible connection module ensures your core dashboard logic remains unchanged while providing flexible database access options.
