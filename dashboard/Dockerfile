# Next.js standalone image. Build from the dashboard/ dir:
#   docker build -t fleet/dashboard:local dashboard/
# API base is baked at build time; the browser reaches query-api via port-forward on localhost:8082.
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ENV NEXT_PUBLIC_API_BASE=http://localhost:8082
RUN npm run build

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production PORT=3000
COPY --from=build /app/.next/standalone ./
COPY --from=build /app/.next/static ./.next/static
COPY --from=build /app/public ./public
EXPOSE 3000
CMD ["node", "server.js"]
