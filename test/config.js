// Set default timezone as UTC
process.env.TZ = 'UTC';

// Set the environment as "testing"
process.env.NODE_ENV = 'testing';

global.config = {
  redis: {
    host: '127.0.0.1',
    port: 6379
  }
};
