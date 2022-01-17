from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.metrics import classification_report, confusion_matrix, precision_score, accuracy_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
import joblib
import os
import json
from tsfresh import select_features, extract_relevant_features
from tsfresh.feature_extraction import ComprehensiveFCParameters


class Machine_learning_model(object):
    def __init__(self, x, y, save_xpath, save_file):
        self.x = x
        self.y = y
        self.save_xpath = save_xpath
        self.save_file = save_file

    def logistic_regression_model(self, test_size=0.3, max_iter=100, assess=True, save=True):
        X, y = self.x, self.y
        min_max_scaler = MinMaxScaler()
        x = min_max_scaler.fit_transform(X)
        if test_size == 0:
            x_train, x_test, y_train, y_test = x, x, y, y
        else:
            x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size)
        model = LogisticRegression(max_iter=max_iter)
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)
        # print(confusion_matrix(y_true=y_test['label'].values, y_pred=y_pred))
        # n = 0
        # for index, y1 in enumerate(y_test['label'].values):
        #     if y1 != y_pred[index]:
        #         print(index, 'True:', y1, 'Pred: ', y_pred[index], '--', y_test.iloc[index])
        #         n += 1
        # print(n)
        if assess:
            self.model_assess(y_test=y_test, y_pred=y_pred)
        if save:
            self.model_save(model, min_max_scaler)

    def decision_tree_model(self, test_size=0.3, assess=True, save=True):
        x, y = self.x, self.y
        min_max_scaler = MinMaxScaler()
        x = min_max_scaler.fit_transform(x)
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size)
        model = DecisionTreeClassifier()
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)
        if assess:
            self.model_assess(y_test=y_test, y_pred=y_pred)
        if save:
            self.model_save(model, min_max_scaler)

    def svm_model(self, test_size=0.3, assess=True, save=True):
        x, y = self.x, self.y
        min_max_scaler = MinMaxScaler()
        x = min_max_scaler.fit_transform(x)
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size)
        model = SVC()
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)
        if assess:
            self.model_assess(y_test=y_test, y_pred=y_pred)
        if save:
            self.model_save(model, min_max_scaler)

    @staticmethod
    def model_assess(y_test, y_pred):
        print("分类报告:", '\n', classification_report(y_true=y_test, y_pred=y_pred))
        print("混淆矩阵:", '\n', confusion_matrix(y_true=y_test, y_pred=y_pred))
        print("准确率:", accuracy_score(y_true=y_test, y_pred=y_pred))
        print("精确率:", precision_score(y_true=y_test, y_pred=y_pred, average=None))
        print("召回率:", recall_score(y_true=y_test, y_pred=y_pred, average=None))

    def model_save(self, model, min_max_scaler):
        save_xpath = self.save_xpath
        save_file = self.save_file
        if not os.path.exists(save_xpath):
            os.makedirs(save_xpath)
        number = 1
        mode = True
        filename = save_file + '_%03d.m' % number
        save_file_list = os.listdir(save_xpath)
        while mode is True:
            if filename in save_file_list:
                number += 1
                filename = save_file + '_%03d.m' % number
            else:
                mode = False
                joblib.dump(model, self.save_xpath + filename)
                joblib.dump(min_max_scaler, self.save_xpath + 'scaler_' + filename)
                print('Save %s successful' % filename)


if __name__ == '__main__':
    import pandas as pd

    x = pd.read_csv('../data/tsfresh_feature.csv')
    x = x.iloc[:, 1:]
    # print(x_)
    y = pd.read_csv('../data/tsfresh_label.csv')['label'].values
    # print(x_)
    # print(y_)
    save_xpath_ = '../param/'
    save_file_ = 'tsfresh_death'
    obj = Machine_learning_model(x, y, save_xpath_, save_file_)
    obj.decision_tree_model()
