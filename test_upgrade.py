#!/usr/bin/env python3
"""
Test script to validate Airflow 3.0.0 upgrade
"""
import os
import sys
import subprocess

def test_airflow_import():
    """Test if Airflow can be imported and shows correct version"""
    try:
        import airflow
        print(f"✓ Airflow imported successfully - Version: {airflow.__version__}")
        return True
    except ImportError as e:
        print(f"✗ Failed to import Airflow: {e}")
        return False

def test_dag_imports():
    """Test if DAGs can be imported without errors"""
    try:
        # Add current directory to Python path
        current_dir = os.path.dirname(os.path.abspath(__file__))
        repo_dir = os.path.dirname(current_dir)
        dags_dir = os.path.join(repo_dir, 'dags')
        sys.path.insert(0, dags_dir)
        
        # Test original DAG
        import streaming_pipeline_demo
        print("✓ Original DAG imported successfully")
        
        # Test new SDK DAG
        import streaming_pipeline_sdk
        print("✓ New SDK DAG imported successfully")
        
        return True
    except Exception as e:
        print(f"✗ Failed to import DAGs: {e}")
        return False

def test_operators_import():
    """Test if custom operators can be imported"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        repo_dir = os.path.dirname(current_dir)
        plugins_dir = os.path.join(repo_dir, 'plugins')
        sys.path.insert(0, plugins_dir)
        
        from kafka_operators import (
            KafkaProducerOperator,
            KafkaConsumerOperator,
            RedisToPostgresOperator,
            DataQualityCheckOperator
        )
        print("✓ Custom operators imported successfully")
        return True
    except Exception as e:
        print(f"✗ Failed to import custom operators: {e}")
        return False

def test_docker_compose_syntax():
    """Test if docker-compose file has valid syntax"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        repo_dir = os.path.dirname(current_dir)
        compose_file = os.path.join(repo_dir, 'docker-compose.yaml')
        
        result = subprocess.run(
            ['docker-compose', '-f', compose_file, 'config'],
            capture_output=True,
            text=True,
            cwd=repo_dir
        )
        
        if result.returncode == 0:
            print("✓ Docker Compose file syntax is valid")
            return True
        else:
            print(f"✗ Docker Compose syntax error: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Failed to validate Docker Compose: {e}")
        return False

def main():
    """Run all tests"""
    print("Running Airflow 3.0.0 upgrade validation tests...\n")
    
    tests = [
        test_airflow_import,
        test_dag_imports,
        test_operators_import,
        test_docker_compose_syntax,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()  # Add spacing
    
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Airflow 3.0.0 upgrade appears successful.")
        return 0
    else:
        print("❌ Some tests failed. Please review the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())