FROM node:20-alpine

WORKDIR /usr/src/app

ENV NODE_ENV=production

RUN apk add --no-cache dumb-init

COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --chown=node:node src ./src

USER node

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["node", "src/consumer.js"]
