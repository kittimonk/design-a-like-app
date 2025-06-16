
import uvicorn
import sys
import os
import socket

def find_free_port(start_port=3000, max_port=3100):
    """Find a free port starting from start_port"""
    for port in range(start_port, max_port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', port))
                return port
        except OSError:
            continue
    raise RuntimeError(f"No free port found between {start_port} and {max_port}")

if __name__ == "__main__":
    # Add current directory to Python path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    # Find an available port
    try:
        backend_port = find_free_port(3000, 3100)
    except RuntimeError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    print(f"Starting FastAPI backend server on port {backend_port}...")
    print(f"API Documentation will be available at: http://localhost:{backend_port}/docs")
    print(f"API base URL: http://localhost:{backend_port}")
    
    # Start the server
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=backend_port,
        reload=True,
        log_level="info"
    )
