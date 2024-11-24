# test/test_broker.py

from aiohttp import web

async def test_broker(request):
    """Test endpoint to check if broker is working."""
    return web.Response(text="Broker is up and running!")

app = web.Application()
app.router.add_get('/test', test_broker)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8080)
