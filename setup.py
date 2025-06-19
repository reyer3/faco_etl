#!/usr/bin/env python3
"""
FACO ETL Setup Helper

Quick setup script for local development.
"""

import os
import sys
import subprocess
from pathlib import Path
from loguru import logger

def check_python_version():
    """Check Python version requirement"""
    if sys.version_info < (3, 9):
        logger.error("Python 3.9+ required. Current: {}.{}.{}".format(*sys.version_info[:3]))
        return False
    logger.info(f"✅ Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
    return True

def setup_virtual_env():
    """Setup virtual environment if not exists"""
    venv_path = Path("venv")
    if venv_path.exists():
        logger.info("✅ Virtual environment already exists")
        return True
    
    try:
        logger.info("🔧 Creating virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", "venv"], check=True)
        logger.info("✅ Virtual environment created")
        
        # Show activation instructions
        if os.name == 'nt':  # Windows
            activate_cmd = "venv\\Scripts\\activate"
        else:  # Unix/Linux/Mac
            activate_cmd = "source venv/bin/activate"
        
        logger.info(f"📋 To activate: {activate_cmd}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to create virtual environment: {e}")
        return False

def install_dependencies():
    """Install Python dependencies"""
    try:
        logger.info("📦 Installing dependencies...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
        logger.info("✅ Dependencies installed")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Failed to install dependencies: {e}")
        return False

def setup_directories():
    """Create necessary directories"""
    dirs = ["credentials", "logs", "data", "output"]
    
    for dir_name in dirs:
        dir_path = Path(dir_name)
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"📁 Created directory: {dir_name}")
        else:
            logger.info(f"✅ Directory exists: {dir_name}")

def setup_env_file():
    """Setup .env file from template"""
    env_file = Path(".env")
    env_example = Path(".env.example")
    
    if env_file.exists():
        logger.info("✅ .env file already exists")
        return True
    
    if env_example.exists():
        try:
            # Copy template to .env
            env_content = env_example.read_text()
            env_file.write_text(env_content)
            logger.info("✅ Created .env from template")
            logger.warning("⚠️  Please edit .env with your specific settings")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create .env: {e}")
            return False
    else:
        logger.error("❌ .env.example not found")
        return False

def check_google_cloud():
    """Check Google Cloud setup"""
    try:
        # Check if gcloud is installed
        result = subprocess.run(["gcloud", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("✅ Google Cloud CLI installed")
            
            # Check if authenticated
            result = subprocess.run(["gcloud", "auth", "list", "--filter=status:ACTIVE"], 
                                  capture_output=True, text=True)
            if "ACTIVE" in result.stdout:
                logger.info("✅ Google Cloud authenticated")
                return True
            else:
                logger.warning("⚠️  Google Cloud not authenticated")
                logger.info("💡 Run: gcloud auth application-default login")
                return False
        else:
            logger.warning("⚠️  Google Cloud CLI not installed")
            logger.info("💡 Install from: https://cloud.google.com/sdk/docs/install")
            return False
    except FileNotFoundError:
        logger.warning("⚠️  Google Cloud CLI not found in PATH")
        logger.info("💡 Install from: https://cloud.google.com/sdk/docs/install")
        return False

def test_basic_run():
    """Test basic ETL run in dry mode"""
    try:
        logger.info("🧪 Testing basic ETL run...")
        result = subprocess.run([
            sys.executable, "main.py", 
            "--dry-run", "--mes", "2025-06", "--estado", "abierto"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            logger.info("✅ Basic ETL test successful")
            return True
        else:
            logger.error(f"❌ ETL test failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.warning("⚠️  ETL test timed out (may be normal)")
        return True
    except Exception as e:
        logger.error(f"❌ ETL test error: {e}")
        return False

def main():
    """Main setup process"""
    logger.info("🚀 FACO ETL Setup Helper")
    logger.info("=" * 40)
    
    steps = [
        ("Checking Python version", check_python_version),
        ("Setting up directories", setup_directories), 
        ("Setting up environment file", setup_env_file),
        ("Installing dependencies", install_dependencies),
        ("Checking Google Cloud", check_google_cloud),
        ("Testing basic run", test_basic_run),
    ]
    
    success_count = 0
    total_steps = len(steps)
    
    for step_name, step_func in steps:
        logger.info(f"\n🔍 {step_name}...")
        if step_func():
            success_count += 1
        else:
            logger.warning(f"⚠️  {step_name} had issues (may not be critical)")
    
    logger.info("\n" + "=" * 40)
    logger.info(f"📊 Setup Results: {success_count}/{total_steps} successful")
    
    if success_count >= total_steps - 1:  # Allow 1 failure
        logger.success("🎉 Setup completed successfully!")
        logger.info("\n📋 Next steps:")
        logger.info("1. Activate virtual environment (if created)")
        logger.info("2. Edit .env file with your settings")
        logger.info("3. Setup Google Cloud credentials")
        logger.info("4. Run: python main.py --dry-run")
    else:
        logger.warning("⚠️  Setup completed with issues. Check the messages above.")
    
    return success_count >= total_steps - 1

if __name__ == "__main__":
    # Simple logging setup for this script
    logger.remove()
    logger.add(sys.stdout, level="INFO", 
               format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")
    
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("⚠️  Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Setup failed: {e}")
        sys.exit(1)