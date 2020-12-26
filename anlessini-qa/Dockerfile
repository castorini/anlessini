FROM public.ecr.aws/lambda/python:3.6

RUN python3.6 -m pip install --no-cache-dir -t . \
    torch==1.5.0+cpu transformers \
    -f https://download.pytorch.org/whl/torch_stable.html

COPY app.py mobilebert-uncased-finetuned-squadv2 ./

# Command can be overwritten by providing a different command in the template directly.
CMD ["app.lambda_handler"]