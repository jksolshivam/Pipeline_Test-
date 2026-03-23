const isRequestValid = require("../middlewares/isRequestValid");
const {
  handleHealth,
  handleHome,
  handleEventRoute,
} = require("../controllers/eventController");

async function eventRoutes(fastify) {
  fastify.get("/health", handleHealth);
  fastify.get("/", handleHome);

  fastify.post(
    "/event/:route",
    { preHandler: isRequestValid },
    handleEventRoute,
  );
}

module.exports = eventRoutes;
