@not(
    or(
        contains(pipeline().parameters.filename, '_2.'),
        or(
            contains(pipeline().parameters.filename, '_3.'),
            or(
                contains(pipeline().parameters.filename, '_4.'),
                or(
                    contains(pipeline().parameters.filename, '_5.'),
                    or(
                        contains(pipeline().parameters.filename, '_6.'),
                        or(
                            contains(pipeline().parameters.filename, '_7.'),
                            or(
                                contains(pipeline().parameters.filename, '_8.'),
                                contains(pipeline().parameters.filename, '_9.')
                            )
                        )
                    )
                )
            )
        )
    )
)
