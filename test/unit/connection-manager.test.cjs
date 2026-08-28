const test = require("node:test");
const assert = require("node:assert/strict");

const {
  RabbitMQConnectionManager,
} = require("../../dist/cjs/config.js");
const {
  SchemaValidationError,
} = require("../../dist/cjs/index.js");

function deferred() {
  let resolve;
  const promise = new Promise((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

function fakeChannel() {
  return {
    closed: false,
    closeCalls: 0,
    onerror: () => {},
    confirmSelect: async () => {},
    close: async function () {
      this.closeCalls++;
      this.closed = true;
    },
  };
}

function fakeConnection(overrides = {}) {
  return {
    closed: false,
    onerror: () => {},
    connect: async function () {
      this.closed = false;
      return this;
    },
    close: async function () {
      this.closed = true;
    },
    channel: async () => fakeChannel(),
    ...overrides,
  };
}

async function raceCloseWithChannelCreation(kind) {
  const opening = deferred();
  const channel = fakeChannel();
  const connection = fakeConnection({ channel: async () => opening.promise });

  const manager = new RabbitMQConnectionManager({
    connector: () => connection,
  });

  let channelPromise;
  if (kind === "regular") channelPromise = manager.getChannel();
  else if (kind === "confirm") channelPromise = manager.getConfirmChannel();
  else if (kind === "disposable") channelPromise = manager.createChannel();
  else {
    const session = await manager.createValidationSession();
    channelPromise = session.createChannel();
  }

  await new Promise((resolve) => setImmediate(resolve));
  await manager.close();
  opening.resolve(channel);

  await assert.rejects(channelPromise, /is closed/);
  assert.equal(channel.closeCalls, 1);
}

test("closes a regular channel that resolves after manager shutdown", async () => {
  await raceCloseWithChannelCreation("regular");
});

test("closes a confirm channel that resolves after manager shutdown", async () => {
  await raceCloseWithChannelCreation("confirm");
});

test("closes a disposable channel that resolves after manager shutdown", async () => {
  await raceCloseWithChannelCreation("disposable");
});

test("closes an isolated validation channel that resolves after manager shutdown", async () => {
  await raceCloseWithChannelCreation("isolated");
});

test("concurrent manager close calls await the same shutdown", async () => {
  const closing = deferred();
  const connection = fakeConnection({ close: () => closing.promise });

  const manager = new RabbitMQConnectionManager({
    connector: () => connection,
  });
  await manager.getConnection();

  const firstClose = manager.close();
  const secondClose = manager.close();
  assert.equal(firstClose, secondClose);

  let completed = false;
  void secondClose.then(() => {
    completed = true;
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(completed, false);

  closing.resolve();
  await Promise.all([firstClose, secondClose]);
  assert.equal(completed, true);
});

test("validation session reuses one isolated connection across channels", async () => {
  let connectorCalls = 0;
  let channelCalls = 0;
  const connection = fakeConnection({
    channel: async () => {
      channelCalls++;
      return fakeChannel();
    },
  });

  const manager = new RabbitMQConnectionManager({
    connector: () => {
      connectorCalls++;
      return connection;
    },
  });

  const session = await manager.createValidationSession();
  const first = await session.createChannel();
  const second = await session.createChannel();
  await first.close();
  await second.close();
  await session.close();
  await manager.close();

  assert.equal(connectorCalls, 1);
  assert.equal(channelCalls, 2);
});

test("SchemaValidationError exposes properties correctly", () => {
  const err = new SchemaValidationError({
    eventName: "test.event",
    eventVersion: "v2",
    eventId: "abc-123",
    originalError: new TypeError("value must be a string"),
  });

  assert.equal(err.name, "SchemaValidationError");
  assert.equal(err.eventName, "test.event");
  assert.equal(err.eventVersion, "v2");
  assert.equal(err.eventId, "abc-123");
  assert.ok(err.originalError instanceof TypeError);
  assert.match(err.message, /test\.event/);
  assert.match(err.message, /vv2/);
  assert.match(err.message, /abc-123/);
  assert.match(err.message, /value must be a string/);
});

test("SchemaValidationError handles non-Error originalError", () => {
  const err = new SchemaValidationError({
    eventName: "other.event",
    eventVersion: "v1",
    eventId: "xyz-789",
    originalError: "just a string reason",
  });

  assert.equal(err.eventName, "other.event");
  assert.match(err.message, /just a string reason/);
});
