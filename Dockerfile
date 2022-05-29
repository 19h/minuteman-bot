FROM rustlang/rust:nightly

WORKDIR /my-source

RUN apt update; apt install -y libclang-dev clang

ADD . /my-source

RUN cd /my-source
RUN cargo rustc --verbose --release
RUN mv /my-source/target/release/minuteman /minuteman

CMD ["/minuteman"]
