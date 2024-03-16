import json

from django.core.serializers import serialize
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.csrf import csrf_exempt
from .processor import main
from .models import ProcessedFiles
from .utils import convert_dtype_to_readable

@csrf_exempt
def file_handler(request):
    if request.method == 'POST':
        file = request.FILES.get('file')
        if file:
            try:
                processed_data = main(file)
                readable_dtypes = {column: convert_dtype_to_readable(dtype) for column, dtype in processed_data.items()}
                print(readable_dtypes)
                save_date = ProcessedFiles.objects.create(
                    file_name=file,
                    completed=True,
                    data_types=readable_dtypes,
                )
                return JsonResponse({'id': save_date.id, 'dtypes': readable_dtypes})
            except Exception as e:
                print(f"An error occurred: {e}")
                save_date = ProcessedFiles.objects.create(
                    file_name=file,
                    completed=False,
                )

                return JsonResponse({'error': 'Server Error'}, status=500)
        return JsonResponse({'error': 'File not provided'}, status=400)

    if request.method == 'PATCH':
        try:
            req = json.loads(request.body)
            print(req)
            file_obj = get_object_or_404(ProcessedFiles, pk=req.get('id'))

            # Update the object's attributes
            file_obj.data_types = req.get('dtypes')

            # Save the object
            file_obj.save()

            p = ProcessedFiles.objects.all()
            for file in p:
                print(file.id)
                print(file.file_name)
                print(file.data_types)

            return JsonResponse({'message': 'Record Updated'})
        except Exception as e:
            print(f"An error occurred: {e}")
            return JsonResponse({'message': 'Server Error'}, status=500)

    if request.method == 'GET':
        try:
            files = ProcessedFiles.objects.order_by('-created_at')
            serialized_data = serialize('json', files)
            response_json = json.loads(serialized_data)

            transformed_response = []



            for item in response_json:
                status = "Failed"
                if item["fields"]["completed"]:
                    status = "Success"
                transformed_item = {
                    "id": item["pk"],
                    "name": item["fields"]["file_name"],
                    "status" : status,
                    "dtypes": item["fields"]["data_types"],
                    "created_at": item["fields"]["created_at"],
                    "updated_at": item["fields"]["updated_at"],
                }
                transformed_response.append(transformed_item)

            return JsonResponse(transformed_response, safe=False)

        except Exception as e:
            print(f"An error occurred: {e}")
            return JsonResponse({'message': 'Server Error'}, status=500)

    return JsonResponse({'error': 'Bad Request'}, status=405)
