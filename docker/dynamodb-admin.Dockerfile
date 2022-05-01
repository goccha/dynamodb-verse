# Accept the Go version for the image to be set as a build argument.
# Default to Go 1.11
ARG NODE_VERSION=16.14.0

# First stage: build the executable.
FROM node:${NODE_VERSION}-alpine3.15

# Create the user and group files that will be used in the running container to
# run the process as an unprivileged user.
RUN mkdir /user && \
    echo 'nobody:x:65534:65534:nobody:/:' > /user/passwd && \
    echo 'nobody:x:65534:' > /user/group

RUN npm install -g dynamodb-admin

EXPOSE 8001

# Perform any further action as an unprivileged user.
USER nobody:nobody

# Run the compiled binary.
CMD ["dynamodb-admin"]