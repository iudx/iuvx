# Vid-IoT
Distributed Video-IoT framework to support modern demands for high fidelity, high throughput, automatable video feeds with support for *IoT data annotated video streams*

<p align="center">
<img src="https://github.com/rbccps-iisc/Vid-IoT/blob/master/docs/vidiot_arch.png">

## Installation
1. Build the docker image. This will trigger a multistage docker build, building ffmpeg and nginx, and deploy the two on an alpine linux image. \
` docker-compose build`
2. Set the necessary environment variables (username, password, db addresses, etc).
3. Bring up the two containers for the main server (lb) and the database(db). \
` docker-compose up lb db`
4. Exec into the lb container and run the server, either as a daemon or on a tmux session. \
` docker exec -it vidiot_lb_l` \
` cd vidiot ` \
` ./scripts/start_all_tmux.sh`
5. The video server should be running and can be attached to by issuing \
` tmux a -t LB` \
` tmux a -t O`
6. A test suite is provided and can be run from inside the LB container. This will test a simple end to end flow. \
` python tests/testSuite.py VidTest.test_simpleFlow`
