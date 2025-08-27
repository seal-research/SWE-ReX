import logging
import subprocess
import time
from typing import Any

from typing_extensions import Self

import shutil, os

from swerex import PACKAGE_NAME, REMOTE_EXECUTABLE_NAME
from swerex.deployment.abstract import AbstractDeployment
from swerex.deployment.config import ApptainerDeploymentConfig
from swerex.deployment.hooks.abstract import CombinedDeploymentHook, DeploymentHook
from swerex.exceptions import DeploymentNotStartedError, DockerPullError
from swerex.runtime.abstract import IsAliveResponse
from swerex.runtime.apptainer import ApptainerRuntime
from swerex.utils.log import get_logger

APPTAINER_BASH = "apptainer" if shutil.which("apptainer") else "singularity"

__all__ = ["ApptainerDeployment", "ApptainerDeploymentConfig"]

class ApptainerDeployment(AbstractDeployment):
    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        **kwargs: Any,
    ):
        """Deployment to local apptainer image and sandbox.

        Args:
            **kwargs: Keyword arguments (see `ApptainerDeploymentConfig` for details).
        """
        self._config = ApptainerDeploymentConfig(**kwargs)
        self._runtime: ApptainerRuntime | None = None
        self.logger = logger or get_logger("rex-deploy")
        self._runtime_timeout = 0.15
        self._hooks = CombinedDeploymentHook()
        self.sandbox_path = None

    def add_hook(self, hook: DeploymentHook):
        self._hooks.add_hook(hook)
    
    @classmethod
    def from_config(cls, config: ApptainerDeploymentConfig) -> Self:
        return cls(**config.model_dump())
    
    async def is_alive(self, *, timeout: float | None = None) -> IsAliveResponse:
        """Checks if the runtime is alive. The return value can be
        tested with bool().

        Raises:
            DeploymentNotStartedError: If the deployment was not started.
        """
        if self._runtime is None:
            return IsAliveResponse(is_alive=False, message="Runtime is None.")
        return await self._runtime.is_alive(timeout=timeout)

    def _pull_image(self) -> str:
        self.logger.info(f"Pulling image {self._config.image!r}")
        self._hooks.on_custom_step("Pulling apptainer image")
        try:
            self.sif_file = self._config.image.replace(":", "_").replace("/", "_")+".sif"
            self.sif_file = str(self._config.apptainer_output_dir / self.sif_file)
            # remove existing sif file if it exists
            if os.path.exists(self.sif_file):
                self.logger.info(f"Removing existing image file {self.sif_file}")
                os.remove(self.sif_file)
            # pull the image
            subprocess.check_output([APPTAINER_BASH, "pull", self.sif_file, self._config.image], stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            msg = f"Failed to pull image {self._config.image}. "
            msg += f"Error: {e.stderr.decode()}"
            msg += f"Output: {e.output.decode()}"
            raise DockerPullError(msg) from e

    def _build_image(self) -> str:
        """Builds image, returns image ID."""
        self.logger.info(
            f"Building image {self._config.image} to install to {self._config.apptainer_output_dir}. "
            "This might take a while (but you only have to do it once). "
        )
        # delete existing sandbox if it exists
        if os.path.exists(self.sandbox_path):
            self.logger.info(f"Removing existing sandbox {self.sandbox_path}")
            shutil.rmtree(self.sandbox_path)
            
        # create sandbox directory
        result = subprocess.run(
                [APPTAINER_BASH, "build", "--sandbox", self.sandbox_path, self.sif_file],
                # cwd=str(build_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        if result.returncode != 0:
            self.logger.error(f"Failed to build Apptainer sandbox image:\n{result.stderr}")
            raise RuntimeError(f"Failed to build Apptainer sandbox image: {result.stderr}")

    async def start(self):
        """Starts the runtime."""
        self._pull_image()
        self.sandbox_path = str(self._config.apptainer_output_dir / "apptainer_sandbox")
        self._build_image()
        
        self._hooks.on_custom_step("Starting runtime")
        self.logger.info(f"Starting runtime")
        self._runtime = ApptainerRuntime(logger=self.logger)
        t0 = time.time()
        self.logger.info(f"Runtime started in {time.time() - t0:.2f}s")
    
    async def stop(self):
        """Stops the runtime."""
        if self._runtime is not None:
            await self._runtime.close()
            self._runtime = None

    @property
    def runtime(self) -> ApptainerRuntime:
        """Returns the runtime if running.

        Raises:
            DeploymentNotStartedError: If the deployment was not started.
        """
        if self._runtime is None:
            raise DeploymentNotStartedError()
        return self._runtime
