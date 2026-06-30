FROM node:22-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY tsconfig.json ./
COPY migrations ./migrations
COPY src ./src
COPY scripts ./scripts

CMD ["npm", "run", "worker"]
