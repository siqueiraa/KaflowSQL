# Security Policy

## Supported Versions

We actively support security updates for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| main    | :white_check_mark: |
| latest  | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

1. **Do not** create a public GitHub issue
2. Email the maintainers directly with details
3. Include steps to reproduce if possible
4. Allow reasonable time for response before disclosure

## Security Best Practices

When deploying KaflowSQL:

### Configuration Security
- Never commit credentials to version control
- Use environment variables for sensitive data
- Regularly rotate access keys and secrets
- Enable S3 checkpointing encryption

### Network Security
- Use TLS for Kafka connections in production
- Restrict network access to required ports
- Use VPC/private networks when possible

### Container Security
- Run containers as non-root user
- Keep base images updated
- Scan images for vulnerabilities
- Use minimal base images

### Monitoring
- Enable audit logging
- Monitor for unusual activity
- Set up alerts for failures
- Regular security reviews

## Response Process

1. **Acknowledgment**: We will acknowledge receipt within 48 hours
2. **Assessment**: Initial assessment within 5 business days
3. **Response**: Coordinated disclosure timeline based on severity
4. **Resolution**: Security patches and advisory publication

Thank you for helping keep KaflowSQL secure!