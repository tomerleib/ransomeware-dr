# Ransomware DR - Cloud Backup Solutions

This repository contains automated backup solutions for both AWS RDS and Google Cloud Platform, designed to enhance disaster recovery capabilities and protect against ransomware attacks. The project was initially developed as an internal tool and has been migrated to this public repository.

## AWS RDS Backup Implementation

The AWS RDS backup solution provides automated snapshot management with enhanced security features:

### Key Features
- Automated RDS snapshot creation and management
- Cross-region snapshot replication for disaster recovery
- Snapshot encryption using AWS KMS
- Cross-account sharing capabilities for secure backup storage
- Automated cleanup of outdated snapshots
- Slack notifications for backup status and errors

### Components
- `aws/rds-automation/` - Main RDS backup implementation
  - `app/rds_backup.py` - Core backup functionality
  - `app/utils/` - Shared utilities and helpers
  - Kubernetes deployment configurations for automated scheduling

### Security Features
- Encrypted snapshots using AWS KMS
- Cross-account sharing for isolation
- RBAC-based access control
- Secure parameter handling

## GCP Backup Implementation

The Google Cloud SQL backup solution focuses on creating a snapshot from one project to another:

### Key Features
- Automated backup creation and management
- Backup validation and integrity checking
- Retention policy management
- Cloud Function-based implementation

### Components
- `gcp/cloud-functions/` - Cloud Functions implementation
  - Backup management functions
  - Validation and integrity checking
  - Multi-region replication logic
  - Retention management

### Security Features
- IAM-based access control
- Cross-project replication capabilities

## Getting Started

### AWS RDS Setup
1. Configure AWS credentials and permissions
2. Update the configuration in `aws/rds-automation/app/utils/global_vars.py`
3. Deploy using the provided Kubernetes configurations
4. Configure Slack notifications (optional)

### GCP Setup
1. Set up GCP service account with appropriate permissions
2. Deploy Cloud Functions using provided configurations
3. Configure backup retention policies
