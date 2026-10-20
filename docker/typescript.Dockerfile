FROM node:24.10.0-bookworm-slim
WORKDIR /core
COPY --from=core /package.json /package-lock.json /tsconfig.json /tsconfig.build.json ./
COPY --from=core /src ./src
RUN npm ci
WORKDIR /app
RUN mkdir -p node_modules/@deckr && ln -s /core node_modules/@deckr/core && ln -s /core/node_modules/@types node_modules/@types
COPY agents/typescript/worker.ts agents/typescript/soak.ts agents/typescript/package.json ./
RUN /core/node_modules/.bin/tsc --noEmit --strict --skipLibCheck --allowImportingTsExtensions --target ES2022 --module NodeNext --moduleResolution NodeNext worker.ts
COPY protocol /protocol
ENV PROTOCOL_DIR=/protocol
ENTRYPOINT ["node", "--experimental-strip-types", "/app/worker.ts"]
