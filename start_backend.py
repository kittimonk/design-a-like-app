
import uvicorn
import sys
import os

if __name__ == "__main__":
    # Add current directory to Python path
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    
    print("Starting FastAPI backend server...")
    print("API Documentation will be available at: http://localhost:3000/docs")
    print("API base URL: http://localhost:3000")
    
    # Start the server
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=3000,
        reload=True,
        log_level="info"
    )
