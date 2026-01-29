# Health check
curl http://localhost:8000/health

# See latest funding rates
curl http://localhost:8000/snapshots/latest

# See recent events/alerts
curl http://localhost:8000/events/recent


curl -X POST http://localhost:8000/leaderboard/send