FROM rustlang/rust:nightly

WORKDIR /my-source

ADD . /my-source

RUN apt update; apt install -y libclang-dev clang

RUN cd /my-source
RUN cargo rustc --verbose --release
RUN mv /my-source/target/release/minuteman /minuteman
RUN rm -rfv /my-source

CMD ["/minuteman"]
