# Builds the dashboard into a minimal runtime image using Next's standalone
# output, which bundles only the node_modules actually reached at runtime.

# ---- dependencies ----
FROM node:22-alpine AS deps

WORKDIR /app

# Only the manifests, so this layer is cached until a dependency changes.
# npm ci installs exactly the lockfile and fails when it has drifted from
# package.json — the behaviour you want in a reproducible build.
COPY package.json package-lock.json ./
RUN npm ci

# ---- build ----
FROM node:22-alpine AS build

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# No API_KEY here, not even a placeholder. Every data route is rendered per
# request, so nothing calls the API at build time — and an image is not a
# secret store: anyone who can pull it can read every layer and every ENV.
# The key is supplied at runtime by the orchestrator.
ENV NEXT_TELEMETRY_DISABLED=1

RUN npm run build

# ---- runtime ----
FROM node:22-alpine AS runtime

WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000
ENV HOSTNAME=0.0.0.0

# A dedicated unprivileged user: nothing here needs root, and a container
# escape should not start from it.
RUN addgroup -g 1001 -S nodejs && adduser -S nextjs -u 1001

# The standalone server does not copy public/ or .next/static itself — they are
# expected to be served by a CDN — so they are copied in explicitly here, since
# this image serves them directly.
COPY --from=build --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=build --chown=nextjs:nodejs /app/.next/static ./.next/static
COPY --from=build --chown=nextjs:nodejs /app/public ./public

USER nextjs

EXPOSE 3000

CMD ["node", "server.js"]
