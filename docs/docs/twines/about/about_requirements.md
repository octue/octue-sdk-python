# Requirements of the framework {#requirements}

A _twine_ must describe a digital twin, and have multiple roles. It
must:

1.  Define what data is required by a digital twin, in order to run
2.  Define what data will be returned by the twin following a successful
    run
3.  Define the formats of these data, in such a way that incoming data
    can be validated
4.  Define what other (1st or 3rd party) twins / services are required
    by this one in order for it to run.

If this weren\'t enough, the description:

1.  Must be trustable (i.e. a _twine_ from an untrusted, corrupt or
    malicious third party should be safe to at least read)
2.  Must be machine-readable _and machine-understandable_[^1]
3.  Must be human-readable _and human-understandable_[^2]
4.  Must be discoverable (that is, searchable/indexable) otherwise
    people won\'t know it\'s there in orer to use it.

Fortunately for digital twin developers, several of these requirements
have already been seen for data interchange formats developed for the
web. **twined** uses `JSON` and `JSONSchema` to help interchange data.

If you\'re not already familiar with `JSONSchema` (or wish to know why
**twined** uses `JSON` over the seemingly more appropriate `XML`
standard), see `introducing_json_schema`{.interpreted-text role="ref"}.

[^1]:
    _Understandable_ essentially means that, once read, the machine or
    human knows what it actually means and what to do with it.

[^2]:
    _Understandable_ essentially means that, once read, the machine or
    human knows what it actually means and what to do with it.
