from multiprocessing import Queue, Process
from rabbitmq.rabbitmq import Creator

import random
import shutil
import yaml
import cv2
import os


try:
    with open(os.path.join(os.path.dirname(os.getcwd()), os.path.join("config", "config.yaml"))) as file:
        config = yaml.safe_load(file)
except Exception as err:
    print(err)


def run_parallel_images():
    """Функция запуска"""
    main_directory = os.path.dirname(os.getcwd())
    path = os.path.join(main_directory, "media", "video")
    first_video = os.path.join(path, "2_video.mp4")
    second_video = os.path.join(path, "3_video.mp4")
    first_folder = os.path.join(main_directory, "media", "2_video")
    second_folder = os.path.join(main_directory, "media", "3_video")
    new_images_folder = os.path.join(main_directory, "media", "epilepsy")
    video_path = os.path.join(path, "new_video.mp4")
    os.mkdir(new_images_folder)

    first_queue = Queue()
    second_queue = Queue()
    merge_queue = Queue()

    p1 = Process(target=read_video, args=(first_video, first_queue, main_directory))
    p2 = Process(target=read_video, args=(second_video, second_queue, main_directory))
    p3 = Process(target=give_size, args=(first_queue, first_folder, merge_queue, new_images_folder))
    p4 = Process(target=give_size, args=(second_queue, second_folder, merge_queue, new_images_folder))
    p5 = Process(target=rabbit_push, args=(merge_queue,))
    p6 = Process(target=create_video, args=(new_images_folder, video_path))

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()

    show_video(video_path)


def read_video(video, frames_queue, main_directory):
    """Функция получения кадров из видео"""
    video_name = video.split("/")[-1].rstrip(".mp4")
    path = os.path.join(main_directory, "media")
    cap = cv2.VideoCapture(video)
    folder = os.path.join(path, video_name)
    if not os.path.exists(folder):
        os.mkdir(folder)
    count = 0
    while cap.isOpened():
        success, image = cap.read()
        if not success or count == 50:
            break
        img_name = video_name + str(count) + ".png"
        path = os.path.join(folder, img_name)
        cv2.imwrite(path, image)
        frames_queue.put(path)
        count += 1
    cap.release()


def give_size(image_queue, folder, merge_queue, recipient_folder):
    """Функия, которая забирает картинки из очереди и сжимает их"""
    count = 0
    while True:
        img = image_queue.get()
        src = cv2.imread(img)
        name = generate_img_name(img.split("/")[-1].rstrip(".png"))
        file_name = os.path.join(recipient_folder, name)
        output = cv2.resize(src, dsize=(300, 300))
        cv2.imwrite(file_name, output)
        merge_queue.put(file_name)
        if count == 49:
            break
        count += 1
    shutil.rmtree(folder)


def rabbit_push(merge_queue):
    """Функция отправки изображений в rabbitmq"""
    count = 0
    while count != 100:
        img = merge_queue.get()
        rabbitmq_creator.send_data(data={"img_path": img, "queue": "video_create"})
        count += 1


def create_video(folder, video_path):
    """Функция создания видео"""
    video = cv2.VideoWriter(video_path, cv2.VideoWriter_fourcc(*'mp4v'), 100, (300, 300))
    count = 0
    while count != 100:
        img = rabbitmq_creator.get_data()
        img = cv2.imread(img["img_path"])
        video.write(img)
        count += 1
    video.release()
    shutil.rmtree(folder)


def show_video(path):
    """Функция отображения видео"""
    window_name = "EPILEPSY"
    cv2.namedWindow(window_name, cv2.WND_PROP_FULLSCREEN)
    cv2.setWindowProperty(window_name, cv2.WND_PROP_FULLSCREEN, cv2.WINDOW_FULLSCREEN)
    cap = cv2.VideoCapture(path)
    while cap.isOpened():
        ret, frame = cap.read()
        if cv2.waitKey(1) & 0xFF == ord('q') or not ret:
            break
        cv2.imshow(window_name, frame)
    cap.release()
    cv2.destroyAllWindows()


def generate_img_name(filename):
    """Функция создания рандомного видео"""
    new_name = ""
    for letter in filename:
        choice = random.choice([True, False])
        if choice:
            new_name = new_name[::-1]
        new_name += letter
    return new_name + '.png'


if __name__ == "__main__":
    rabbitmq_creator = Creator(config['RABBITMQ'])
    rabbitmq_creator.start()
    run_parallel_images()
