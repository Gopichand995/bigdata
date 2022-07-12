import numpy as np
import imageio
import scipy.ndimage as sc
import cv2

img_path = "C:/Users/GopichandBarri/Desktop/kgp.jpg"


def rgb2gray(rgb):
    return np.dot(rgb[..., :3], [0.2989, 0.5870, 0.1140])


def dodge(front, back):
    final_sketch = front * 255 / (255 - back)
    final_sketch[final_sketch > 255] = 255
    final_sketch[back == 255] = 255
    return final_sketch.astype("uint8")


ss = imageio.imread(img_path)
gray = rgb2gray(ss)
i = 255 - gray
blur = sc.filters.gaussian_filter(i, sigma=15)
r = dodge(blur, gray)
cv2.imwrite("C:/Users/GopichandBarri/Desktop/kgp_sketch.jpg", r)
