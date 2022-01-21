# Take Home

### How to run the project
- Install packages
  
      pip install -r requirments.txt
- Run docker compose   
    
      docker-compose up    
- Run pgAdmin
    
      http://127.0.0.1:5050
      email: admin@admin.com
      password: root
  
  _*Note:*_ you can check Postgres container IP address by running the following command:
      
      docker ps <container_id> | grep IPAddress
      default IPAddress is 172.18.0.3

- Run the script
    
      python main.py
new one
