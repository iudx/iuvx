from flask import Flask, request
from pyIUDX.auth import auth
app = Flask(__name__)

@app.route('/auth/<authserver>/<user>/<token>')
def hlsvideoauth(authserver,user,token):
   
   print (request.headers.get('X-Original-URI'))
   print (request.headers.get('X-Real-IP'))
   print (request.headers.get('X-Forwarded-For'))

   iudx_token = authserver + "/" + user + "/" + token
   print (iudx_token)
   dir(auth)
   iudx_auth = auth.Auth("soumitripal@iisc.ac.in-class-1-certificate.pem","private-key.pem")
   response = iudx_auth.introspect_token(iudx_token)
   print(response)
   if response["success"]:
        print("Success");
        return iudx_token, 200
   else:
        err_msg = response["response"]["error"]
        return err_msg, 400

@app.route('/rtmpauth', methods=['GET'])
def rtmpvideoauth():
   
    if request.method == "GET":
        print (request.headers.get('X-Original-URI'))
        print (request.headers.get('X-Real-IP'))
        print (request.headers.get('X-Forwarded-For'))
        print (request.headers.get('Content-Type'))
     
        iudx_token = request.args.get('token')
        print (iudx_token)
        dir(auth)
        iudx_auth = auth.Auth("soumitripal@iisc.ac.in-class-1-certificate.pem","private-key.pem")
        response = iudx_auth.introspect_token(iudx_token)
        print(response)
        if response["success"]:
             print("Success");
             return iudx_token, 200
        else:
             err_msg = response["response"]["error"]
             return err_msg, 400

if __name__ == '__main__':
   app.run(host="0.0.0.0",debug = True, port=9000)
