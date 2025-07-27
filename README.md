
<p align="center">
  <img src="./docs/logo/aurorawire-dark-logo.png#gh-dark-mode-only" alt="AuroraWire-Logo" width="40%" />
  <img src="./docs/logo/aurorawire-light-logo.png#gh-light-mode-only" alt="AuroraWire-Logo" width="40%" />
</p>

## AuroraWire - Enterprise-grade PostgreSQL wire protocol proxy

<div align="center">

**Enterprise-grade PostgreSQL wire protocol proxy for AWS Aurora Serverless v2**

AuroraWire connects pgAdmin, DBeaver, and any PostgreSQL client to Aurora Serverless without exposing your database publicly or managing EC2 instances.

[Getting Started](#-quick-start) •
[Examples](#-usage-examples) •
[Contributing](#-contributing)

</div>

---

## 🎯 Why AuroraWire?

### The Problem
- **Security Risk**: Making RDS publicly accessible exposes your database to attacks and breaches
- **Infrastructure Overhead**: Running bastion hosts or VPN connections requires EC2 management
- **Development Friction**: Connecting local tools to Aurora Serverless is complex and insecure
- **Connection Management**: Manual connection pooling and socket handling complexity

### The Solution
AuroraWire eliminates these challenges by providing a **zero-infrastructure middleware** that acts as a secure bridge between your PostgreSQL clients and Aurora Serverless v2.

## ✨ Key Benefits

<table>
<tr>
<td width="33%">

### 🔒 **Enhanced Security**
- **Zero Public Exposure**: Your AWS RDS cluster remains completely private
- **DDoS Protection**: Get out from security data breaches and DDoS attacks on RDS cluster links
- **No Attack Surface**: Eliminates direct database connection vulnerabilities
- **AWS IAM Integration**: Leverages native AWS security controls

</td>
<td width="33%">

### 🚀 **Zero Infrastructure**
- **No EC2 Required**: Eliminate bastion hosts and VPN complexities
- **No Connection Pool Management**: Data API acts as your intelligent middleware
- **Serverless-First**: Built for Aurora Serverless v2 architecture
- **Instant Setup**: Deploy in minutes, not hours

</td>
<td width="33%">

### 🛠️ **Developer Experience**
- **Universal Compatibility**: Works with pgAdmin, DBeaver, psql, and any PostgreSQL client
- **Development Ready**: Perfect for local development against Aurora Serverless
- **Socket Control**: Middleware handles all PostgreSQL socket connections seamlessly
- **Production Ready**: Enterprise-grade reliability and performance

</td>
</tr>
</table>

## 🌟 Who Should Use This?

### **Enterprise Teams**
- Running AWS RDS from your own infrastructure and need secure access
- Eliminating public database exposure for compliance requirements
- Reducing operational overhead of bastion hosts and VPN management

### **Development Teams**
- Want to connect interface tools (pgAdmin, DBeaver) to Aurora Serverless v2
- Need local development access to production-like Aurora environments
- Seeking simplified database connectivity without infrastructure complexity

### **Security-Conscious Organizations**
- Prioritizing database security and attack surface reduction
- Meeting strict compliance requirements for database access
- Implementing zero-trust database connectivity patterns

---

## 🚀 Quick Start

### Prerequisites
- Node.js 18+
- AWS Account with RDS Data API enabled
- Aurora Serverless v2 PostgreSQL cluster
- IAM credentials with Data API permissions

### Installation

```bash
# Clone the repository
git clone https://github.com/bhoobalan-bhoo/aws-aurora-wire-proxy.git
cd aws-aurora-wire-proxy

# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Edit .env with your AWS and Aurora details

# Start the proxy
npm start
```

### Connect with pgAdmin
1. **Host**: `localhost`
2. **Port**: `5432`
3. **Database**: Your Aurora database name
4. **Username/Password**: Any value (proxy handles authentication)

**That's it!** Your Aurora Serverless v2 cluster is now accessible through pgAdmin without any public exposure.

---

## 🏗️ Architecture

```mermaid
graph LR
    A[pgAdmin/DBeaver] --> B[AuroraWire]
    B --> C[AWS RDS Data API]
    C --> D[Aurora Serverless v2]

    B -.-> E[PostgreSQL Wire Protocol]
    C -.-> F[HTTPS/JSON API]
    D -.-> G[Private VPC Network]

    %% Color styles
    style A fill:#bbdefb,stroke:#1e88e5,stroke-width:2px,color:#0d47a1
    style B fill:#e1bee7,stroke:#8e24aa,stroke-width:2px,color:#4a148c
    style C fill:#ffe0b2,stroke:#fb8c00,stroke-width:2px,color:#e65100
    style D fill:#c8e6c9,stroke:#43a047,stroke-width:2px,color:#1b5e20

    style E fill:#f5f5f5,stroke:#9e9e9e,color:#424242,stroke-dasharray: 5 5
    style F fill:#f5f5f5,stroke:#9e9e9e,color:#424242,stroke-dasharray: 5 5
    style G fill:#ede7f6,stroke:#673ab7,color:#311b92

```

## 📊 Performance & Limitations

| Feature | Support Level | Notes |
|---------|---------------|-------|
| **Basic Queries** | ✅ Full | SELECT, INSERT, UPDATE, DELETE |
| **Transactions** | ⚠️ Limited | Data API transaction limitations |
| **Connection Pooling** | ✅ Simulated | Handled by Data API middleware |
| **SSL/TLS** | ✅ Full | End-to-end encryption |
| **Large Result Sets** | ⚠️ Paginated | 15-minute query timeout |
| **PostgreSQL Extensions** | ❌ Limited | Depends on Aurora Serverless support |

---

## 📁 Project Structure

```
aws-aurora-wire-proxy/
└── src/
   ├── config/           # AWS & logging configuration
   ├── data-api/         # RDS Data API client
   ├── protocol/         # PostgreSQL wire protocol implementation
   ├── server/           # Proxy server & connection handling
   ├── translation/      # Query translation & metadata
   ├── utils/            # Utilities & constants
   └── index.js          # Main File
```

## 🔧 Configuration

### Environment Variables

```bash
# AWS Configuration
AWS_REGION=""
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""

# RDS Data API Configuration
RDS_CLUSTER_ARN=""
RDS_SECRET_ARN=""
RDS_DATABASE_NAME=""
```

### IAM Permissions

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "rds-data:ExecuteStatement",
                "rds-data:BatchExecuteStatement",
                "rds-data:BeginTransaction",
                "rds-data:CommitTransaction",
                "rds-data:RollbackTransaction"
            ],
            "Resource": "arn:aws:rds:*:*:cluster:your-cluster-name"
        }
    ]
}
```

---

## 💡 Usage Examples

### Connect to Aurora with AuroraWire

### pgAdmin Connection
```bash
# pgAdmin Server Configuration
Host: localhost
Port: 5432
Maintenance Database: your_db_name
Username: proxy
Password: proxy
```

### Command Line (psql)
```bash
psql -h localhost -p 5432 -U proxy -d your_database_name
```

### Application Connection String
```javascript
const connectionString = 'postgresql://proxy:proxy@localhost:5432/your_db_name';
```

---

## 🛡️ Security Features

### **Private Database Access**
- Aurora cluster remains in private subnets
- No public IP addresses or security group modifications needed
- Zero direct internet exposure to your database

### **Attack Surface Reduction**
- Eliminates direct database connection vulnerabilities
- Protection against SQL injection attacks targeting connection strings
- No exposed database ports or endpoints

### **AWS Native Security**
- Leverages AWS IAM for authentication and authorization
- Uses AWS Secrets Manager for credential management
- All traffic encrypted with AWS TLS standards

---

## 🚀 Deployment Options

### **Local Development**
```bash
npm run dev
```

### **Docker Container**
```bash
Cooooking!
```

### **Production Deployment**
```bash
Cooooking!
```

---


## 🤝 Contributing

We welcome contributions!

### Development Setup
```bash
# Fork and clone the repo
git clone https://github.com/your-username/aws-aurora-wire-proxy.git

# Install dependencies
npm install

# Run in development mode
npm run dev

```

---

## 📈 Roadmap

- [ ] **Enhanced Transaction Support** - Extended Data API transaction capabilities
- [ ] **Query Result Caching** - Redis-based result caching for improved performance  
- [ ] **Connection Multiplexing** - Advanced connection management and pooling
- [ ] **Monitoring Dashboard** - Real-time metrics and health monitoring
- [ ] **Kubernetes Operator** - Native Kubernetes deployment and management
- [ ] **Multi-Region Support** - Cross-region Aurora cluster support
- [ ] **GraphQL Interface** - GraphQL-to-SQL translation layer

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **PostgreSQL Community** - For comprehensive protocol documentation
- **AWS Team** - For RDS Data API and Aurora Serverless innovations
- **Open Source Contributors** - Building the ecosystem that makes this possible

---

<div align="center">

**⭐ Star this repository if AuroraWire helped you build secure, scalable database connectivity!**

**Made with ❤️ for the developer community**

[Report Bug](https://github.com/bhoobalan-bhoo/aws-aurora-wire-proxy/issues) •
[Request Feature](https://github.com/bhoobalan-bhoo/aws-aurora-wire-proxy/issues) •

</div>
