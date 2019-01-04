# pingpubsub
A protocol for pinging with publish-suscribe

The idea is that you have a server reachable by clients, but the agents are not reachable from the server. However, you want to reach them
and something like websockets is too expensive or you are not using http at all.

In this scenario what you have to do is run a pubsub server somewhere reachable by your server and your clients. The clients connect to
the pubsub using a id shared with your server (it will be used to identify your clients). Now the server can ask the pubsub server to
"ping" a particular client, and then the client can contact the server in a normal way to find out why it was notified.

If you need to handle many clients, you can run brokers. A broker accepts clients and can connect to a pubsub server or to another broker.
That way, you can spread the load (and the amount of open connections).

Missing:
- SSL Support
- Testing
- A better publisher and subscriber class
