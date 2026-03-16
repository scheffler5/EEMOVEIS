FROM mongo:7.0

# Limita o cache do WiredTiger para economizar RAM em dev/container
# O entrypoint oficial do mongo:7.0 ainda é usado (gerencia init e auth)
ENV MONGO_DATA_DIR=/data/db

EXPOSE 27017

CMD ["mongod", "--bind_ip_all", "--quiet", "--wiredTigerCacheSizeGB", "0.25"]