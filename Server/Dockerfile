FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

RUN apt-get update && apt-get install -y clang zlib1g-dev

COPY . .

WORKDIR /src/Server
RUN dotnet publish -c Release -o /app

FROM alpine:latest
RUN apk add --no-cache libstdc++ gcompat zlib
WORKDIR /app

COPY --from=build /app .

RUN mkdir -p ./Storage/Incomplete ./Storage/Final

ENTRYPOINT ["./HIRTP-Link"]