def run(analysis, *args, **kwargs):
    """Your main entrypoint to run the application

    This is the function that gets run each time somebody requests an analysis from the digital twin / data service.
    You should write your own code and call it from here.

    It needs to be called 'run' and the file must be called 'app.py'; Octue will handle the rest, supplying
    you with an "analysis" object with validated inputs for you to process.

    ## The Analysis:

    `analysis` is an instantiated Analysis class object, which you can import here (as shown) or anywhere else in your
    code. It contains:
        - ``configuration_values``, which have been validated against the twine
        - ``configuration_manifest``, a Manifest instance whose contents have been validated and whose files have been
           checked (files are checked to be present and, if a `sha` field is given in the manifest, their
           contents checked to match the sha)
        - ``input_values``, which have been validated against the twine
        - ``input_manifest``, a Manifest instance whose contents have been validated and whose files have been
           checked (files are checked to be present and, if a `sha` field is given in the manifest, their
           contents checked to match the sha)
        - ``output_values``, dict which can be added to as required (on completion, it will be validated
           against the twine and returned to the requester)
        - ``output_manifest``, a Manifest instance to which newly created datasets and files should be added (on
           completion, the presence of the files will be checked, their shas calculated and they'll be returned to the
           requester or uploaded)
        - ``children``, a dict of Child objects allowing you to access child twins/services
        - ``credentials``, a dict of Credential objects

    """
    analysis.logger.info("Hello! The child services template app is running!")

    elevations = analysis.children["elevation"].ask(input_values=analysis.input_values, timeout=20)["output_values"]
    wind_speeds = analysis.children["wind_speed"].ask(input_values=analysis.input_values, timeout=20)["output_values"]

    analysis.logger.info(
        "The wind speeds and elevations at %s are %s and %s.",
        analysis.input_values["locations"],
        elevations,
        wind_speeds,
    )

    analysis.output_values = {"wind_speeds": wind_speeds, "elevations": elevations}
