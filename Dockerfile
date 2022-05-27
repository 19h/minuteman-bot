FROM rustlang/rust:nightly

ADD . /my-source

RUN cd /my-source
RUN cargo rustc --verbose --release \
RUN mv /my-source/target/release/minuteman /minuteman \
RUN rm -rfv /my-source

CMD ["/minuteman"]
