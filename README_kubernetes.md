1. Start minikube
```  minikube start```   

2. cd into kubernetes files cd docker-kubernetes-tasks/kubernetes-files and mount the volume of dags folder from host machine to minikube and airflow will take it from minikube.
  ``` minikube mount ./dags/:/mnt/airflow/dags ```

3. Script container 2 deployments files and 2 services for airflow and postgres service.
  ``` chmod +x ./script-create.sh ./script-create.sh```
   

4. Pods will now be created and be accessible by following commands
 ``` kubectl get pods```
   

5. Enter into bash mode of airflow container. Write airflow container name in the place of pod.
  ```kubectl exec -it <pod> -- bash ```
   

6. Fernet key error will be thrown if following command is not used. This is to export Fernet key variable to env variable.
  ``` FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")``` ```export FERNET_KEY=$FERNET_KEY```
   

7. Initiate the database of airflow in postgres.
  ```airflow initdb```
   

8. Port forward to using kubectl to listen 8080 on minikube on your host machine.
  ```kubectl port-forward svc/<service>  8080:8080 ```
  

 
