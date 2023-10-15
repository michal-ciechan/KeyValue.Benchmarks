FROM redis
COPY redis.conf /usr/local/etc/redis/redis.conf
RUN chmod -R 777 /usr/local/etc/redis/redis.conf
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]