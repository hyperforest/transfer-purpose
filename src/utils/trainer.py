import torch
from tqdm import tqdm
from sklearn.metrics import f1_score
from time import time


class Trainer:
    def __init__(self, model, optimizer, criterion, device):
        self.model = model
        self.optimizer = optimizer
        self.criterion = criterion
        self.device = device

    def preprocess(self, inputs, labels):
        inputs = torch.reshape(inputs, (-1, inputs.shape[-1]))
        labels = torch.reshape(labels, (-1, self.model.num_classes))
        return inputs, labels

    def train_step(self, inputs, labels):
        self.model.train()
        inputs, labels = inputs.to(self.device), labels.to(self.device)
        inputs, labels = self.preprocess(inputs, labels)

        self.optimizer.zero_grad()
        outputs = self.model(inputs)
        loss = self.criterion(outputs, labels)
        loss.backward()
        self.optimizer.step()

        preds = torch.argmax(outputs, dim=1)
        f1 = f1_score(torch.argmax(labels, dim=1), preds, average="macro")
        metrics = {
            "loss": loss.item(),
            "f1": f1,
        }

        return metrics, preds, outputs

    def test_step(self, inputs, labels):
        self.model.eval()
        inputs, labels = inputs.to(self.device), labels.to(self.device)
        inputs, labels = self.preprocess(inputs, labels)

        outputs = self.model(inputs)
        loss = self.criterion(outputs, labels)

        preds = torch.argmax(outputs, dim=1)
        f1 = f1_score(torch.argmax(labels, dim=1), preds, average="macro")
        metrics = {
            "loss": loss.item(),
            "f1": f1,
        }
        # print(preds)

        return metrics, preds, outputs

    def fit(self, train_loader, valid_loader, epochs, verbose=0):
        history = {
            'loss': [],
            'f1': [],
            'val_loss': [],
            'val_f1': []
        }

        for epoch in range(epochs):
            time_st = time()

            text_prefix = f"[Train] [Epoch {epoch + 1}/{epochs}] "
            _ = self.train(
                train_loader, verbose=(verbose == 2), text_prefix=text_prefix
            )

            text_prefix = f"[Eval ] [Epoch {epoch + 1}/{epochs}] "
            train_metrics, train_preds = self.eval(
                train_loader,
                verbose=(verbose >= 2),
                text_prefix=text_prefix,
                metrics_prefix="",
            )

            text_prefix = f"[Eval ] [Epoch {epoch + 1}/{epochs}] "
            val_metrics, val_preds = self.eval(
                valid_loader,
                verbose=(verbose >= 2),
                text_prefix=text_prefix,
                metrics_prefix="val_",
            )

            history['loss'].append(train_metrics['loss'])
            history['f1'].append(train_metrics['f1'])
            history['val_loss'].append(val_metrics['val_loss'])
            history['val_f1'].append(val_metrics['val_f1'])

            time_en = time()
            runtime = time_en - time_st

            if verbose >= 1:
                metrics = {**train_metrics, **val_metrics}
                text = " - ".join([f"{k}: {v:.4f}" for k, v in metrics.items()])
                print(f"[Epoch {epoch + 1}/{epochs}] {runtime:.1f}s - {text}")

            if verbose == 2:
                print()

        return history

    def train(self, train_loader, verbose=True, text_prefix=None):
        text_prefix = text_prefix or ""
        pbar = train_loader

        targets, predictions, outputs = [], [], []

        self.model.train()

        if verbose:
            pbar = tqdm(train_loader)

        for data in pbar:
            inputs, labels = data
            metrics, preds, output = self.train_step(inputs, labels)

            labels = torch.reshape(labels, (-1, self.model.num_classes))
            targets.append(torch.argmax(labels, dim=1))
            predictions.append(preds)
            outputs.append(output)

            if verbose:
                text = f"{text_prefix}" + " ".join(
                    [f"{k}: {v:.4f}" for k, v in metrics.items()]
                )
                pbar.set_description_str(text)

        targets = torch.cat(targets)
        predictions = torch.cat(predictions)
        outputs = torch.cat(outputs)
        # print(targets.shape, predictions.shape)

        loss = self.criterion(outputs, targets).item()
        f1 = f1_score(targets, predictions, average="macro")

        metrics = {
            "loss": loss,
            "f1": f1,
        }
        return metrics, predictions

    def eval(self, test_loader, verbose=True, text_prefix=None, metrics_prefix=None):
        text_prefix = text_prefix or ""
        metrics_prefix = metrics_prefix or ""

        targets, predictions, outputs = [], [], []

        self.model.eval()

        pbar = test_loader
        if verbose:
            pbar = tqdm(test_loader)

        with torch.no_grad():
            for data in pbar:
                inputs, labels = data
                metrics, preds, output = self.test_step(inputs, labels)

                labels = torch.reshape(labels, (-1, self.model.num_classes))
                targets.append(torch.argmax(labels, dim=1))
                predictions.append(preds)
                outputs.append(output)

                if verbose:
                    text = f"{text_prefix}" + " ".join(
                        [f"{metrics_prefix}{k}: {v:.4f}" for k, v in metrics.items()]
                    )
                    pbar.set_description_str(text)

        targets = torch.cat(targets)
        predictions = torch.cat(predictions)
        outputs = torch.cat(outputs)
        
        loss = self.criterion(outputs, targets).item()
        f1 = f1_score(targets, predictions, average="macro")
        
        metrics = {
            f"{metrics_prefix}loss": loss,
            f"{metrics_prefix}f1": f1,
        }
        return metrics, predictions

    def save(self, path):
        torch.save(self.model.state_dict(), path)

    def load(self, path):
        self.model.load_state_dict(torch.load(path))
