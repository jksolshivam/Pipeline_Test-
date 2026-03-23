const { requiredFieldValidation, enqueueEvent } = require("../utils");

const handleHealth = (request, reply) => {
  reply.send({ status: "ok" });
};

const handleHome = (request, reply) => {
  reply.send({ status: "online" });
};

const handleEventRoute = (request, reply) => {
  const { route: routeName } = request.params;
  const { packageSecret, rawBody, headers, payloadJson } = request;
  const route = request.server.configManager.getRoute(routeName);

  if (!route) {
    enqueueEvent(
      "event-unregistered-route",
      { headers, raw_body: rawBody },
      request.server.producer,
    );
    return reply.status(202).send({ status: "ok" });
  }

  if (!packageSecret) {
    enqueueEvent(
      "event-unlisted-package-id",
      { headers, raw_body: rawBody },
      request.server.producer,
    );
    return reply.status(202).send({ status: "ok" });
  }
  const validation = requiredFieldValidation(payloadJson, route.requiredFields);
  if (!validation.isValid) {
    return reply.status(400).send({ error: "Invalid event payload" });
  }

  enqueueEvent(route.topic, payloadJson, request.server.producer);
  reply.status(202).send({ status: "ok" });
};

module.exports = {
  handleHealth,
  handleHome,
  handleEventRoute,
};
