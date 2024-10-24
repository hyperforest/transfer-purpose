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
        inputs, labels = inputs.to(self.device), labels.to(self.device)
        inputs, labels = self.preprocess(inputs, labels)

        self.optimizer.zero_grad()
        outputs = self.model(inputs)
        loss = self.criterion(outputs, labels)
        loss.backward()
        self.optimizer.step()

        preds = torch.argmax(outputs, dim=1)
        # f1 = f1_score(torch.argmax(labels, dim=1), preds, average="macro")
        metrics = {
            "loss": loss.item(),
            # "f1": f1,
        }

        return metrics, preds, outputs

    def test_step(self, inputs, labels):
        inputs, labels = inputs.to(self.device), labels.to(self.device)
        inputs, labels = self.preprocess(inputs, labels)

        outputs = self.model(inputs)
        loss = self.criterion(outputs, labels)

        preds = torch.argmax(outputs, dim=1)
        # f1 = f1_score(torch.argmax(labels, dim=1), preds, average="macro")
        metrics = {
            "loss": loss.item(),
            # "f1": f1,
        }
        # print(preds)

        return metrics, preds, outputs

    def fit(
        self,
        train_loader,
        valid_loader,
        epochs,
        verbose=0,
        reduce_lr=False,
        reduce_lr_factor=0.9,
        reduce_lr_patience=10,
    ):
        history = {"loss": [], "f1": [], "val_loss": [], "val_f1": []}

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

            history["loss"].append(train_metrics["loss"])
            history["f1"].append(train_metrics["f1"])
            history["val_loss"].append(val_metrics["val_loss"])
            history["val_f1"].append(val_metrics["val_f1"])

            time_en = time()
            runtime = time_en - time_st

            if verbose >= 1:
                metrics = {**train_metrics, **val_metrics}
                text = " - ".join([f"{k}: {v:.4f}" for k, v in metrics.items()])
                print(f"[Epoch {epoch + 1}/{epochs}] {runtime:.1f}s - {text}")

            if verbose == 2:
                print()

            # update optimizer LR
            if epoch > 0 and epoch % reduce_lr_patience == 0:
                for param_group in self.optimizer.param_groups:
                    param_group["lr"] = param_group["lr"] * reduce_lr_factor

        return history

    def train(self, train_loader, verbose=True, text_prefix=None):
        text_prefix = text_prefix or ""
        pbar = train_loader

        self.model.train()

        if verbose:
            pbar = tqdm(train_loader)

        running_loss = 0.0
        for i, data in enumerate(pbar):
            inputs, labels = data
            metrics, preds, output = self.train_step(inputs, labels)

            labels = torch.reshape(labels, (-1, self.model.num_classes))
            running_loss += metrics["loss"]

            if verbose:
                text = f"{text_prefix}loss: {running_loss / (i + 1):.4f}"
                pbar.set_description_str(text)

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
