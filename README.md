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

The Google Cloud Platform backup solution focuses on secure cloud storage management:

### Key Features
- Automated backup creation and management
- Multi-region replication for disaster recovery
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
- Cloud KMS encryption
- IAM-based access control
- Secure key management
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
4. Set up monitoring and alerting

## Configuration

### AWS Configuration
```yaml
# Example configuration
region: us-east-1
backup_retention_days: 30
cross_account_id: "123456789012"
notification_channel: "#backup-alerts"
```

### GCP Configuration
```yaml
# Example configuration
project_id: your-project-id
backup_bucket: backup-storage
retention_days: 30
replication_regions:
  - us-central1
  - europe-west1
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Note

This project was initially developed as an internal tool and has been migrated to this public repository for broader use and collaboration.