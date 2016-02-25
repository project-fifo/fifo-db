REBAR=./rebar3

.PHONY: all test

all: compile

compile:
	$(REBAR) compile

test:
	$(REBAR) eunit

clean:
	$(REBAR) clean


dialyzer:
	$(REBAR) dialyzer
