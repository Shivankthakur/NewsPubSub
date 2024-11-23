from aiohttp import web
from data_store import DataStore
from replication import DataReplication
from election import LeaderElection
from heartbeat import Heartbeat

data_store = DataStore()
replication = DataReplication(data_store)
leader_election = LeaderElection()
heartbeat = Heartbeat()

# API Routes
async def publish(request):
    """Publish a message to a topic."""
    data = await request.json()
    topic = data.get('topic')
    message = data.get('message')
    data_store.store_message(topic, message)
    
    # Replicate message to other brokers
    await replication.replicate_message(topic, message)
    
    return web.json_response({'status': 'success'})

async def subscribe(request):
    """Subscribe to a topic and get all messages."""
    topic = request.match_info.get('topic')
    messages = data_store.get_messages(topic)
    return web.json_response({'topic': topic, 'messages': messages})

async def heartbeat(request):
    """Heartbeat endpoint for failure detection."""
    return web.json_response({'status': 'alive'})

async def leader(request):
    """Get the current leader broker."""
    leader_id = leader_election.get_current_leader()
    return web.json_response({'leader': leader_id})

# Initialize web application
def create_app():
    app = web.Application()
    app.router.add_post('/publish', publish)
    app.router.add_get('/subscribe/{topic}', subscribe)
    app.router.add_get('/heartbeat', heartbeat)
    app.router.add_get('/leader', leader)
    
    # Start background tasks
    app.on_startup.append(heartbeat.start_heartbeat)
    app.on_startup.append(leader_election.start_leader_election)
    
    return app

if __name__ == '__main__':
    web.run_app(create_app(), host='0.0.0.0', port=8080)
