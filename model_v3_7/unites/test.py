import joblib
import numpy as np
from param import DEATH_MODEL_FILE, DEATH_SCALER_FILE


def test(x, death_model_file=DEATH_MODEL_FILE,death_scaler_file=DEATH_SCALER_FILE):
    min_max_scaler = joblib.load(death_scaler_file)
    X = min_max_scaler.transform(x)
    model = joblib.load(death_model_file)
    prediction = model.predict(X)
    return prediction


if __name__ == '__main__':
    death_model_file_ = 'param/death_pig_model_014.m'
    death_scaler_file_ = 'param/scaler_death_pig_model_014.m'
    x = np.array([[0.96, 0.93, 0.73, 27.94, 31.59, -3.66]])
    r = test(x, death_model_file_, death_scaler_file_)
    print(r)
