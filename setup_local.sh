#!/bin/bash

# FACO ETL Local Setup Script
# Creates necessary directories and files for local development

echo "🔧 FACO ETL - Local Setup"
echo "========================="

# Create logs directory
echo "📁 Creating logs directory..."
mkdir -p logs
echo "✅ logs/ created"

# Create credentials directory
echo "📁 Creating credentials directory..."
mkdir -p credentials
echo "✅ credentials/ created"

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "⚙️  Creating .env file..."
    cp .env.example .env
    echo "✅ .env created from .env.example"
    echo "📝 Please edit .env with your settings"
else
    echo "✅ .env already exists"
fi

# Create placeholder for credentials
if [ ! -f "credentials/key.json" ]; then
    echo "🔑 Creating credentials placeholder..."
    cat > credentials/key.json << 'EOF'
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "your-private-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----\n",
  "client_email": "your-service-account@your-project.iam.gserviceaccount.com",
  "client_id": "your-client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs/your-service-account%40your-project.iam.gserviceaccount.com"
}
EOF
    echo "⚠️  Placeholder credentials/key.json created"
    echo "📝 Replace with your actual Google Cloud service account key"
else
    echo "✅ credentials/key.json already exists"
fi

# Check Python environment
echo ""
echo "🐍 Checking Python environment..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo "✅ $PYTHON_VERSION found"
    
    # Check if virtual environment exists
    if [ -d ".venv" ] || [ -d "venv" ] || [ ! -z "$VIRTUAL_ENV" ]; then
        echo "✅ Virtual environment detected"
    else
        echo "⚠️  No virtual environment detected"
        echo "💡 Recommend creating one:"
        echo "   python3 -m venv .venv"
        echo "   source .venv/bin/activate  # Linux/Mac"
        echo "   .venv\\Scripts\\activate     # Windows"
    fi
else
    echo "❌ Python 3 not found"
fi

# Set permissions
echo ""
echo "🔒 Setting permissions..."
chmod +x validate.sh
echo "✅ validate.sh executable"

echo ""
echo "========================="
echo "🎯 Setup complete! Next steps:"
echo ""
echo "1. Install dependencies:"
echo "   pip install -r requirements.txt"
echo ""
echo "2. Edit configuration:"
echo "   nano .env"
echo ""
echo "3. Add Google Cloud credentials:"
echo "   # Replace credentials/key.json with your actual service account key"
echo ""
echo "4. Test the setup:"
echo "   python main.py --dry-run"
echo ""
echo "5. Run validation:"
echo "   ./validate.sh"
echo ""
echo "🚀 Happy coding!"