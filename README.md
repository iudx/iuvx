# IUVX - Indian Urban Video Exchange (Vid-IoT)
Distributed Video-IoT framework to support modern demands for high fidelity, high throughput, automatable video feeds with support for *IoT data annotated video streams*

<p align="center">
<img src="https://github.com/iudx/iuvx/blob/single-node/docs/vidiot_arch.png">

## Installation
1. Build the docker image. This will trigger a multistage docker build, building ffmpeg and nginx, and deploy the two on an alpine linux image. \
` docker-compose build`
2. Set the necessary environment variables (username, password, db addresses, etc).
3. Bring up the two containers for the main server (lb) and the database(db). \
` docker-compose up lb db`
4. Exec into the lb container and run the server, either as a daemon or on a tmux session. \
` docker exec -it vid-iot_lb_l` \
` cd vidiot ` \
` ./scripts/start_all_tmux.sh`
5. The video server should be running and can be attached to by issuing \
` tmux a -t LB` \
` tmux a -t O`
6. A test suite is provided and can be run from inside the LB container. This will test a simple end to end flow. \
` python tests/testSuite.py VidTest.test_simpleFlow`

## APIs
The openAPI specifications can be found [here](./vidiot.json).

## Network Specific Configurations
If you are running docker on a VM (such as kvm based), you may need to manually add an iptables forwarding rule  
` sudo iptables -t nat -A PREROUTING -i <iface> -p tcp --dport 1935 -j DNAT --to <vm-ip>:1935`  
` sudo iptables -t nat -A PREROUTING -i <iface> -p tcp --dport 8080 -j DNAT --to <vm-ip>:8080`  
And the VM will need to add a firewall rule
`sudo ufw allow 1935/tcp`  
`sudo ufw allow 8080/tcp`  

## Visualizing Stream status
You can visualize the streaams through grafana.  
Install using docker again   
`docker volume create grafana-storage`  
`docker run -d -p 3000:3000 -v grafana-storage:/var/lib/grafana grafana/grafana`  
Add an influxdb datasource with db "statter" and db url and credentials from `./scripts/docker_env.env`  
Import the json settings from [here](./scripts/grafana_dashboard.json).
